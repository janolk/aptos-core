// Copyright © Aptos Foundation

use std::collections::BTreeMap;
use std::io;
use std::io::{Chain, Error, ErrorKind};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt, StreamExt};
use tokio::runtime::Handle;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Receiver;
use aptos_config::config::{DiscoveryMethod, HANDSHAKE_VERSION, NetworkConfig, Peer, PeerRole, PeerSet, RoleType};
use aptos_config::network_id::{NetworkContext, NetworkId, PeerNetworkId};
use aptos_crypto::x25519;
use aptos_event_notifications::{DbBackedOnChainConfig,EventSubscriptionService};
use aptos_logger::{error, info, warn};
#[cfg(any(test, feature = "testing", feature = "fuzzing"))]
use aptos_netcore::transport::memory::MemoryTransport;
use aptos_netcore::transport::tcp::{TCPBufferCfg, TcpSocket, TcpTransport};
use aptos_netcore::transport::{ConnectionOrigin, Transport};
use aptos_network2::application::ApplicationCollector;
use aptos_network2::application::interface::OutboundRpcMatcher;
use aptos_network2::application::metadata::PeerMetadata;
// use aptos_network_discovery::DiscoveryChangeListener;
use aptos_time_service::TimeService;
use aptos_types::chain_id::ChainId;
use aptos_network2::application::storage::PeersAndMetadata;
use aptos_network2::connectivity_manager::{ConnectivityManager, ConnectivityRequest};
use aptos_network2::logging::NetworkSchema;
use aptos_network2::noise::stream::NoiseStream;
use aptos_network2::{counters, peer};
use aptos_network2::protocols::wire::handshake::v1::{ProtocolId, ProtocolIdSet};
use aptos_network2::protocols::wire::messaging::v1::NetworkMessage;
use aptos_network2::protocols::network::{OutboundPeerConnections, PeerStub, ReceivedMessage};
use aptos_network2::transport::{APTOS_TCP_TRANSPORT, AptosNetTransport, AptosNetTransportActual, Connection};
use aptos_network_discovery::DiscoveryChangeListener;
use aptos_short_hex_str::AsShortHexStr;
use aptos_types::account_address::AccountAddress;
use aptos_types::network_address::{NetworkAddress, Protocol};
use aptos_types::PeerId;
use tokio_retry::strategy::ExponentialBackoff;

// use peer::Peer;

#[derive(Debug, PartialEq, PartialOrd)]
enum State {
    CREATED,
    BUILT,
    STARTED,
}


/// Inbound and Outbound connections are always secured with NoiseIK.  The dialer
/// will always verify the listener.
#[derive(Debug)]
pub enum AuthenticationMode {
    /// Inbound connections will first be checked against the known peers set, and
    /// if the `PeerId` is known it will be authenticated against it's `PublicKey`
    /// Otherwise, the incoming connections will be allowed through in the common
    /// pool of unknown peers.
    MaybeMutual(x25519::PrivateKey),
    /// Both dialer and listener will verify public keys of each other in the
    /// handshake.
    Mutual(x25519::PrivateKey),
}

/// Build Network module with custom configuration values.
/// Methods can be chained in order to set the configuration values.
/// MempoolNetworkHandler and ConsensusNetworkHandler are constructed by calling
/// [`NetworkBuilder::build`].  New instances of `NetworkBuilder` are obtained
/// via [`NetworkBuilder::create`].
pub struct NetworkBuilder {
    state: State,
    executor: Option<Handle>,
    time_service: TimeService,
    network_context: NetworkContext,
    chain_id: ChainId,
    config: NetworkConfig,
    discovery_listeners: Vec<DiscoveryChangeListener<DbBackedOnChainConfig>>,
    peers_and_metadata: Arc<PeersAndMetadata>,
    apps: Arc<ApplicationCollector>,
    peer_senders: Arc<OutboundPeerConnections>,
    handle: Option<Handle>,
    // temporarily hold a value from create() until start()
    connectivity_req_rx: Option<tokio::sync::mpsc::Receiver<ConnectivityRequest>>,
}

impl NetworkBuilder {
    /// Create a new NetworkBuilder based on the provided configuration.
    pub fn create(
        chain_id: ChainId,
        role: RoleType,
        config: &NetworkConfig,
        time_service: TimeService,
        reconfig_subscription_service: Option<&mut EventSubscriptionService>,
        peers_and_metadata: Arc<PeersAndMetadata>,
        peer_senders: Arc<OutboundPeerConnections>,
        handle: Option<Handle>,
    ) -> NetworkBuilder {
        let network_context = NetworkContext::new(role, config.network_id, config.peer_id());
        let (connectivity_req_sender, connectivity_req_rx) = tokio::sync::mpsc::channel::<ConnectivityRequest>(10);
        let mut nb = NetworkBuilder{
            state: State::CREATED,
            executor: None,
            time_service,
            network_context,
            chain_id,
            config: config.clone(),
            discovery_listeners: vec![],
            peers_and_metadata,
            peer_senders,
            apps: Arc::new(ApplicationCollector::new()), // temporary empty app set
            handle,
            connectivity_req_rx: Some(connectivity_req_rx),
        };
        nb.setup_discovery(reconfig_subscription_service, connectivity_req_sender);
        nb
    }

    pub fn set_apps(&mut self, apps: Arc<ApplicationCollector>) {
        self.apps = apps;
    }

    pub fn active_protocol_ids(&self) -> ProtocolIdSet {
        let mut out = ProtocolIdSet::empty();
        for (protocol_id, _) in self.apps.iter() {
            out.insert(*protocol_id);
        }
        out
    }

    pub fn build(&mut self, handle: Handle) {
        if self.state != State::CREATED {
            panic!("NetworkBuilder.build but not in state CREATED");
        }
        self.handle = Some(handle);
        self.state = State::BUILT;
    }

    fn setup_discovery(
        &mut self,
        mut reconfig_subscription_service: Option<&mut EventSubscriptionService>,
        conn_mgr_reqs_tx: tokio::sync::mpsc::Sender<ConnectivityRequest>,
    ) {
        for disco in self.config.discovery_methods().into_iter() {
            let listener = match disco {
                DiscoveryMethod::Onchain => {
                    let reconfig_events = reconfig_subscription_service
                        .as_mut()
                        .expect("An event subscription service is required for on-chain discovery!")
                        .subscribe_to_reconfigurations()
                        .expect("On-chain discovery is unable to subscribe to reconfigurations!");
                    let identity_key = self.config.identity_key();
                    let pubkey = identity_key.public_key();
                    DiscoveryChangeListener::validator_set(
                        self.network_context,
                        conn_mgr_reqs_tx.clone(),
                        pubkey,
                        reconfig_events,
                    )
                }
                DiscoveryMethod::File(file_discovery) => DiscoveryChangeListener::file(
                    self.network_context,
                    conn_mgr_reqs_tx.clone(),
                    file_discovery.path.as_path(),
                    Duration::from_secs(file_discovery.interval_secs),
                    self.time_service.clone(),
                ),
                DiscoveryMethod::Rest(rest_discovery) => DiscoveryChangeListener::rest(
                    self.network_context,
                    conn_mgr_reqs_tx.clone(),
                    rest_discovery.url.clone(),
                    Duration::from_secs(rest_discovery.interval_secs),
                    self.time_service.clone(),
                ),
                DiscoveryMethod::None => {
                    continue;
                }
            };
            self.discovery_listeners.push(listener);
        }
    }

    fn get_tcp_buffers_cfg(&self) -> TCPBufferCfg {
        TCPBufferCfg::new_configs(
            self.config.inbound_rx_buffer_size_bytes,
            self.config.inbound_tx_buffer_size_bytes,
            self.config.outbound_rx_buffer_size_bytes,
            self.config.outbound_tx_buffer_size_bytes,
        )
    }

    fn build_transport(&mut self) -> (TransportPeerManager, AptosNetTransportActual) {
        let listen_parts = self.config.listen_address.as_slice();
        let key = self.config.identity_key();
        let mutual_auth = self.config.mutual_authentication;
        let protos = self.active_protocol_ids();
        let enable_proxy_protocol = self.config.enable_proxy_protocol;
        match listen_parts[0] {
            Protocol::Ip4(_) | Protocol::Ip6(_) => {
                // match listen_parts[1]
                let mut aptos_tcp_transport = APTOS_TCP_TRANSPORT.clone();
                let tcp_cfg = self.get_tcp_buffers_cfg();
                aptos_tcp_transport.set_tcp_buffers(&tcp_cfg);
                let ant = AptosNetTransport::<TcpTransport>::new(
                    aptos_tcp_transport,
                    self.network_context,
                    self.time_service.clone(),
                    key,
                    self.peers_and_metadata.clone(),
                    mutual_auth,
                    HANDSHAKE_VERSION,
                    self.chain_id,
                    protos,
                    enable_proxy_protocol,
                );
                let pm = PeerManager::new(ant.clone(), self.peers_and_metadata.clone(), self.config.clone(), self.network_context, self.apps.clone(), self.peer_senders.clone());
                (TransportPeerManager::Tcp(pm), AptosNetTransportActual::Tcp(ant))
            }
            #[cfg(any(test, feature = "testing", feature = "fuzzing"))]
            Protocol::Memory(_) => {
                let ant = AptosNetTransport::new(
                    MemoryTransport,
                    self.network_context,
                    self.time_service.clone(),
                    key,
                    self.peers_and_metadata.clone(),
                    mutual_auth,
                    HANDSHAKE_VERSION,
                    self.chain_id,
                    protos,
                    enable_proxy_protocol,
                );
                let pm = PeerManager::new(ant.clone(), self.peers_and_metadata.clone(), self.config.clone(), self.network_context, self.apps.clone(), self.peer_senders.clone());
                (TransportPeerManager::Memory(pm), AptosNetTransportActual::Memory(ant))
            }
            _ => {
                panic!("cannot listen on address {:?}", self.config.listen_address);
            }
        }
    }

    pub fn start(&mut self) {
        if self.state != State::BUILT {
            panic!("NetworkBuilder.build but not in state BUILT");
        }
        let handle = self.handle.clone().unwrap();
        handle.enter();
        let seeds = self.config.merge_seeds();
        let connectivity_req_rx = self.connectivity_req_rx.take().unwrap();
        let (tpm, ant) = self.build_transport();
        let cm = ConnectivityManager::new(
            self.config.clone(),
            self.network_context,
            self.time_service.clone(),
            self.peers_and_metadata.clone(),
            seeds,
            connectivity_req_rx,
            ExponentialBackoff::from_millis(self.config.connection_backoff_base).factor(1000),
            ant,
            self.apps.clone(),
            self.peer_senders.clone(),
        );
        handle.spawn(cm.start(handle.clone()));
        for disco in self.discovery_listeners.drain(..) {
            disco.start(&handle);
        }
        let listen_addr = transport_peer_manager_start(
            tpm,
            self.config.listen_address.clone(),
            handle,
            self.network_context.network_id(),
            self.apps.clone(),
        );
        info!("network {:?} listening on {:?}", self.network_context.network_id(), listen_addr);
        self.state = State::STARTED;
    }

    pub fn network_context(&self) -> NetworkContext {
        self.network_context.clone()
    }
}

// single function to wrap variants of templated enum
fn transport_peer_manager_start(
    tpm: TransportPeerManager,
    listen_address: NetworkAddress,
    executor: Handle,
    network_id: NetworkId,
    apps: Arc<ApplicationCollector>,
) -> NetworkAddress {
    let result = match tpm {
        TransportPeerManager::Tcp(mut pm) => { pm.listen( listen_address,  executor ) }
        #[cfg(any(test, feature = "testing", feature = "fuzzing"))]
        TransportPeerManager::Memory(mut pm) => { pm.listen( listen_address,  executor ) }
    };
    match result {
        Ok(listen_address) => { listen_address }
        Err(err) => {
            panic!("could not start network {:?}: {:?}", network_id, err);
        }
    }
}

/// PeerManager might be more correctly "peer listener" in the new framework?
/// TODO: move into network/framework2
struct PeerManager<TTransport, TSocket>
    where
        TTransport: Transport,
        TSocket: AsyncRead + AsyncWrite,
{
    transport: TTransport,
    peers_and_metadata: Arc<PeersAndMetadata>,
    peer_cache: Vec<(PeerNetworkId,PeerMetadata)>,
    peer_cache_generation: u32,
    config: NetworkConfig,
    network_context: NetworkContext,
    apps: Arc<ApplicationCollector>,
    peer_senders: Arc<OutboundPeerConnections>,
    _ph2 : PhantomData<TSocket>,
}

// TODO: move into network/framework2
impl<TTransport, TSocket> PeerManager<TTransport, TSocket>
    where
        TTransport: Transport<Output = Connection<TSocket>> + Send + 'static,
        TSocket: aptos_network2::transport::TSocket,
{
    pub fn new(
        transport: TTransport,
        peers_and_metadata: Arc<PeersAndMetadata>,
        config: NetworkConfig,
        network_context: NetworkContext,
        apps: Arc<ApplicationCollector>,
        peer_senders: Arc<OutboundPeerConnections>,
    ) -> Self {
        Self{
            transport,
            peers_and_metadata,
            peer_cache: vec![],
            peer_cache_generation: 0,
            config,
            network_context,
            apps,
            peer_senders,
            _ph2: Default::default(),
        }
    }

    fn maybe_update_peer_cache(&mut self) {
        // if no update is needed, this should be very fast
        // otherwise make copy of peers for use by this thread/task
        if let Some((update, update_generation)) = self.peers_and_metadata.get_all_peers_and_metadata_generational(self.peer_cache_generation, true, &[]) {
            self.peer_cache = update;
            self.peer_cache_generation = update_generation;
        }
    }

    fn listen(
        mut self,
        listen_addr: NetworkAddress,
        executor: Handle,
    ) -> Result<NetworkAddress, <TTransport>::Error> {
        let (sockets, listen_addr_actual) = executor.block_on(self.first_listen(listen_addr))?;
        info!("listener_thread to spawn ({:?})", listen_addr_actual);
        executor.spawn(self.listener_thread(sockets, executor.clone()));
        Ok(listen_addr_actual)
    }

    async fn first_listen(&mut self, listen_addr: NetworkAddress) -> Result<(<TTransport>::Listener, NetworkAddress), TTransport::Error> {
        self.transport.listen_on(listen_addr)
    }

    async fn listener_thread(mut self, mut sockets: <TTransport>::Listener, executor: Handle) {
        // TODO: leave some connection that can close and shutdown this listener?
        info!("listener_thread start");
        loop {
            let (conn_fut, remote_addr) = match sockets.next().await {
                Some(result) => match result {
                    Ok(conn) => { conn }
                    Err(err) => {
                        error!("listener_thread {:?} got err {:?}, exiting", self.config.network_id, err);
                        return;
                    }
                }
                None => {
                    info!("listener_thread {:?} got None, assuming source closed, exiting", self.config.network_id, );
                    return;
                }
            };
            match conn_fut.await {
                Ok(mut connection) => {
                    let ok = self.check_new_inbound_connection(&connection);
                    info!("listener_thread got connection {:?}, ok={:?}", remote_addr, ok);
                    if !ok {
                        // conted and logged inside check function above, just close here and be done.
                        connection.socket.close();
                        continue;
                    }
                    let remote_peer_network_id = PeerNetworkId::new(self.network_context.network_id(), connection.metadata.remote_peer_id);
                    peer::start_peer(
                        &self.config,
                        connection.socket,
                        connection.metadata,
                        self.apps.clone(),
                        executor.clone(),
                        remote_peer_network_id,
                        self.peers_and_metadata.clone(),
                        self.peer_senders.clone(),
                        self.network_context,
                    );
                }
                Err(err) => {
                    error!("listener_thread {:?} connection post-processing failed (continuing): {:?}", self.config.network_id, err);
                }
            }
        }
    }

    // is the new inbound connection okay? => true
    // no, we should disconnect => false
    fn check_new_inbound_connection(&mut self, conn: &Connection<TSocket>) -> bool {
        // Everything below here is meant for unknown peers only. The role comes from
        // the Noise handshake and if it's not `Unknown` then it is trusted.
        // TODO: do more checking for 'trusted' peers
        if conn.metadata.role != PeerRole::Unknown {
            return true;
        }

        // Count unknown inbound connections
        self.maybe_update_peer_cache();
        let mut unknown_inbound_conns = 0;
        let mut already_connected = false;
        let remote_peer_id = conn.metadata.remote_peer_id;

        if remote_peer_id == self.network_context.peer_id() {
            debug_assert!(false, "Self dials shouldn't happen");
            warn!(
                NetworkSchema::new(&self.network_context)
                    .connection_metadata_with_address(&conn.metadata),
                "Received self-dial, disconnecting it"
            );
            return false;
        }

        for wat in self.peer_cache.iter() {
            if wat.0.peer_id() == remote_peer_id {
                already_connected = true;
            }
            let remote_metadata = wat.1.get_connection_metadata();
            if remote_metadata.origin == ConnectionOrigin::Inbound && remote_metadata.role == PeerRole::Unknown {
                unknown_inbound_conns += 1;
            }
        }

        // Reject excessive inbound connections made by unknown peers
        // We control outbound connections with Connectivity manager before we even send them
        // and we must allow connections that already exist to pass through tie breaking.
        if !already_connected
            && unknown_inbound_conns + 1 > self.config.max_inbound_connections
        {
            info!(
                NetworkSchema::new(&self.network_context)
                .connection_metadata_with_address(&conn.metadata),
                "{} Connection rejected due to connection limit: {}",
                self.network_context,
                conn.metadata
            );
            counters::connections_rejected(&self.network_context, conn.metadata.origin).inc();
            return false;
        }

        if already_connected {
            // TODO network2: old code at network/framework/src/peer_manager/mod.rs PeerManager::add_peer() line 615 had provision for sometimes keeping the new connection, but this simplifies and always _drops_ the new connection
            info!(
                NetworkSchema::new(&self.network_context)
                .connection_metadata_with_address(&conn.metadata),
                "{} Closing incoming connection with Peer {} which is already connected",
                self.network_context,
                remote_peer_id.short_str()
            );
            return false;
        }

        return true;
    }
}


type TcpPeerManager = PeerManager<AptosNetTransport<TcpTransport>, NoiseStream<TcpSocket>>;
#[cfg(any(test, feature = "testing", feature = "fuzzing"))]
type MemoryPeerManager =
PeerManager<AptosNetTransport<MemoryTransport>, NoiseStream<aptos_memsocket::MemorySocket>>;

enum TransportPeerManager {
    Tcp(TcpPeerManager),
    #[cfg(any(test, feature = "testing", feature = "fuzzing"))]
    Memory(MemoryPeerManager),
}
