use napi_derive::napi;

#[napi]
pub fn hello(name: String) -> String {
    format!("hello {name} from hyperiondb-client")
}
