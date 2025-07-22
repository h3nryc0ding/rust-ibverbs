use std::io;

pub fn read_json<T: serde::de::DeserializeOwned, R: io::Read>(reader: &mut R) -> io::Result<T> {
    let mut buf = [0u8; 1024];
    let n = reader.read(&mut buf)?;
    serde_json::from_slice(&buf[..n]).map_err(From::from)
}

pub fn write_json<T: serde::Serialize, W: io::Write>(writer: &mut W, val: &T) -> io::Result<()> {
    let data = serde_json::to_vec(val)?;
    writer.write_all(&data)?;
    writer.flush()
}
