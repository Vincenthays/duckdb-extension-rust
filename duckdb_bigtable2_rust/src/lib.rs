use duckdb::{
    vtab::{
        BindInfo, DataChunk, Free, FunctionInfo, InitInfo, Inserter, LogicalType, LogicalTypeId,
        VTab,
    },
    Connection, Result,
};
use std::{
    error::Error,
    ffi::{c_char, c_void, CString},
};

#[repr(C)]
struct HelloBindData {
    name: *mut c_char,
}

impl Free for HelloBindData {
    fn free(&mut self) {
        unsafe {
            if self.name.is_null() {
                return;
            }
            drop(CString::from_raw(self.name));
        }
    }
}

#[repr(C)]
struct HelloInitData {
    done: bool,
}

struct HelloVTab;

impl Free for HelloInitData {}

impl VTab for HelloVTab {
    type InitData = HelloInitData;
    type BindData = HelloBindData;

    unsafe fn bind(bind: &BindInfo, data: *mut HelloBindData) -> Result<(), Box<dyn Error>> {
        bind.add_result_column("pe_id", LogicalType::new(LogicalTypeId::UBigint));
        bind.add_result_column("title", LogicalType::new(LogicalTypeId::Varchar));
        bind.add_result_column("price", LogicalType::new(LogicalTypeId::Float));
        bind.add_result_column("unit_price", LogicalType::new(LogicalTypeId::Float));
        bind.add_result_column("base_price", LogicalType::new(LogicalTypeId::Float));
        let param = bind.get_parameter(0).to_string();
        (*data).name = CString::new(param).unwrap().into_raw();
        Ok(())
    }

    unsafe fn init(_: &InitInfo, data: *mut HelloInitData) -> Result<(), Box<dyn Error>> {
        (*data).done = false;
        Ok(())
    }

    unsafe fn func(func: &FunctionInfo, output: &mut DataChunk) -> Result<(), Box<dyn Error>> {
        let init_info = func.get_init_data::<HelloInitData>();
        let bind_info = func.get_bind_data::<HelloBindData>();

        unsafe {
            if (*init_info).done {
                output.set_len(0);
            } else {
                (*init_info).done = true;
                let name = CString::from_raw((*bind_info).name);
                let name = name.to_str()?;

                let vec_pe_id = output.flat_vector(0);
                let vec_title = output.flat_vector(1);
                let vec_price = output.flat_vector(2);
                let vec_base_price = output.flat_vector(3);
                let vec_unit_price = output.flat_vector(4);

                vec_pe_id.insert(0, 123_u64.to_be_bytes().as_slice());
                vec_title.insert(0, CString::new(format!("title {name}"))?);
                vec_price.insert(0, 1.3_f32.to_be_bytes().as_slice());
                vec_base_price.insert(0, 1.5_f32.to_be_bytes().as_slice());
                vec_unit_price.insert(0, 1.7_f32.to_be_bytes().as_slice());

                output.set_len(1);
            }
        }
        Ok(())
    }

    fn named_parameters() -> Option<Vec<(String, LogicalType)>> {
        Some(vec![("title".to_string(), LogicalType::new(LogicalTypeId::Varchar))])
    }
}

pub fn _bigtable2_init(conn: Connection) -> Result<()> {
    conn.register_table_function::<HelloVTab>("hello")?;
    Ok(())
}

#[no_mangle]
pub unsafe extern "C" fn bigtable2_rust_init(db: *mut c_void) {
    let connection = Connection::open_from_raw(db.cast()).expect("can't open db connection");
    _bigtable2_init(connection).expect("init failed");
}
