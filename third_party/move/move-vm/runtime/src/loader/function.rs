// Copyright (c) The Diem Core Contributors
// Copyright (c) The Move Contributors
// SPDX-License-Identifier: Apache-2.0

use super::ModuleStorageAdapter;
use crate::{
    loader::{
        access_specifier_loader::load_access_specifier, Loader, Module, Resolver, ScriptHash,
    },
    native_functions::{NativeFunction, NativeFunctions, UnboxedNativeFunction},
};
use move_binary_format::{
    access::ModuleAccess,
    binary_views::BinaryIndexedView,
    errors::{PartialVMError, PartialVMResult},
    file_format::{AbilitySet, Bytecode, CompiledModule, FunctionDefinitionIndex, Visibility},
};
use move_core_types::{identifier::Identifier, language_storage::ModuleId, vm_status::StatusCode};
use move_vm_types::loaded_data::{
    runtime_access_specifier::AccessSpecifier,
    runtime_types::{StructIdentifier, Type},
};
use std::{fmt::Debug, sync::Arc};

// A simple wrapper for the "owner" of the function (Module or Script)
#[derive(Clone, Debug)]
pub(crate) enum Scope {
    Module(ModuleId),
    Script(ScriptHash),
}

// A runtime function representation.
pub struct Function {
    #[allow(unused)]
    pub(crate) file_format_version: u32,
    pub(crate) index: FunctionDefinitionIndex,
    pub(crate) code: Vec<Bytecode>,
    pub ty_param_abilities: Vec<AbilitySet>,
    // TODO: Make `native` and `def_is_native` become an enum.
    pub(crate) native: Option<NativeFunction>,
    pub(crate) def_is_native: bool,
    pub def_is_friend_or_private: bool,
    pub(crate) scope: Scope,
    pub(crate) name: Identifier,
    pub return_tys: Vec<Type>,
    pub(crate) local_tys: Vec<Type>,
    pub param_tys: Vec<Type>,
    pub(crate) access_specifier: AccessSpecifier,
}

// This struct must be treated as an identifier for a function and not somehow relying on
// the internal implementation.
pub struct LoadedFunction {
    pub(crate) module: Arc<Module>,
    pub(crate) function: Arc<Function>,
}

impl std::fmt::Debug for Function {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("Function")
            .field("scope", &self.scope)
            .field("name", &self.name)
            .finish()
    }
}

impl Function {
    pub(crate) fn new(
        natives: &NativeFunctions,
        index: FunctionDefinitionIndex,
        module: &CompiledModule,
        signature_table: &[Vec<Type>],
        struct_names: &[StructIdentifier],
    ) -> PartialVMResult<Self> {
        let def = module.function_def_at(index);
        let handle = module.function_handle_at(def.function);
        let name = module.identifier_at(handle.name).to_owned();
        let module_id = module.self_id();
        let def_is_friend_or_private = match def.visibility {
            Visibility::Friend | Visibility::Private => true,
            Visibility::Public => false,
        };
        let (native, def_is_native) = if def.is_native() {
            (
                natives.resolve(
                    module_id.address(),
                    module_id.name().as_str(),
                    name.as_str(),
                ),
                true,
            )
        } else {
            (None, false)
        };
        let scope = Scope::Module(module_id);
        // Native functions do not have a code unit
        let code = match &def.code {
            Some(code) => code.code.clone(),
            None => vec![],
        };
        let ty_param_abilities = handle.type_parameters.clone();
        let return_tys = signature_table[handle.return_.0 as usize].clone();
        let local_tys = if let Some(code) = &def.code {
            let mut local_tys = signature_table[handle.parameters.0 as usize].clone();
            local_tys.append(&mut signature_table[code.locals.0 as usize].clone());
            local_tys
        } else {
            vec![]
        };
        let param_tys = signature_table[handle.parameters.0 as usize].clone();

        let access_specifier = load_access_specifier(
            BinaryIndexedView::Module(module),
            signature_table,
            struct_names,
            &handle.access_specifiers,
        )?;

        Ok(Self {
            file_format_version: module.version(),
            index,
            code,
            ty_param_abilities,
            native,
            def_is_native,
            def_is_friend_or_private,
            scope,
            name,
            local_tys,
            return_tys,
            param_tys,
            access_specifier,
        })
    }

    #[allow(unused)]
    pub(crate) fn file_format_version(&self) -> u32 {
        self.file_format_version
    }

    pub(crate) fn module_id(&self) -> Option<&ModuleId> {
        match &self.scope {
            Scope::Module(module_id) => Some(module_id),
            Scope::Script(_) => None,
        }
    }

    pub(crate) fn index(&self) -> FunctionDefinitionIndex {
        self.index
    }

    pub(crate) fn get_resolver<'a>(
        &self,
        loader: &'a Loader,
        module_store: &'a ModuleStorageAdapter,
    ) -> Resolver<'a> {
        match &self.scope {
            Scope::Module(module_id) => {
                let module = module_store
                    .module_at(module_id)
                    .expect("ModuleId on Function must exist");
                Resolver::for_module(loader, module_store, module)
            },
            Scope::Script(script_hash) => {
                let script = loader.get_script(script_hash);
                Resolver::for_script(loader, module_store, script)
            },
        }
    }

    pub(crate) fn local_count(&self) -> usize {
        self.local_tys.len()
    }

    pub(crate) fn param_count(&self) -> usize {
        self.param_tys.len()
    }

    pub(crate) fn name(&self) -> &str {
        self.name.as_str()
    }

    pub(crate) fn code(&self) -> &[Bytecode] {
        &self.code
    }

    pub(crate) fn ty_arg_abilities(&self) -> &[AbilitySet] {
        &self.ty_param_abilities
    }

    pub(crate) fn local_tys(&self) -> &[Type] {
        &self.local_tys
    }

    pub(crate) fn return_tys(&self) -> &[Type] {
        &self.return_tys
    }

    pub(crate) fn param_tys(&self) -> &[Type] {
        &self.param_tys
    }

    pub(crate) fn pretty_string(&self) -> String {
        match &self.scope {
            Scope::Script(_) => "Script::main".into(),
            Scope::Module(id) => format!(
                "0x{}::{}::{}",
                id.address().to_hex(),
                id.name().as_str(),
                self.name.as_str()
            ),
        }
    }

    pub(crate) fn is_native(&self) -> bool {
        self.def_is_native
    }

    pub(crate) fn is_friend_or_private(&self) -> bool {
        self.def_is_friend_or_private
    }

    pub(crate) fn get_native(&self) -> PartialVMResult<&UnboxedNativeFunction> {
        self.native.as_deref().ok_or_else(|| {
            PartialVMError::new(StatusCode::MISSING_DEPENDENCY)
                .with_message(format!("Missing Native Function `{}`", self.name))
        })
    }
}

//
// Internal structures that are saved at the proper index in the proper tables to access
// execution information (interpreter).
// The following structs are internal to the loader and never exposed out.
// The `Loader` will create those struct and the proper table when loading a module.
// The `Resolver` uses those structs to return information to the `Interpreter`.
//

// A function instantiation.
#[derive(Clone, Debug)]
pub(crate) struct FunctionInstantiation {
    // index to `ModuleCache::functions` global table
    pub(crate) handle: FunctionHandle,
    pub(crate) instantiation: Vec<Type>,
}

#[derive(Clone, Debug)]
pub(crate) enum FunctionHandle {
    Local(Arc<Function>),
    Remote { module: ModuleId, name: Identifier },
}
