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
        bind.add_result_column("pe_id", LogicalType::new(LogicalTypeId::Integer));
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

                let vec_pe_id = output.flat_vector(0);
                let vec_title = output.flat_vector(1);
                let vec_price = output.flat_vector(2);
                let vec_base_price = output.flat_vector(3);
                let vec_unit_price = output.flat_vector(4);

                for i in 0..10 {
                    vec_pe_id.insert(i, 123_i32.to_be_bytes().as_slice());
                    vec_title.insert(i, CString::new(format!("title {i}"))?);
                    vec_price.insert(i, 1.3_f32.to_be_bytes().as_slice());
                    vec_base_price.insert(i, 1.5_f32.to_be_bytes().as_slice());
                    vec_unit_price.insert(i, 1.7_f32.to_be_bytes().as_slice())
                }

                output.set_len(10);
            }
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalType>> {
        Some(vec![LogicalType::new(LogicalTypeId::Varchar)])
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
