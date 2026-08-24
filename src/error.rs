use std::fmt;



#[derive(Debug)]
pub enum ConrtollerError{
    IO_Error(std::io::Error)
}


impl From<std::io::Error> for ConrtollerError{
    fn from(value: std::io::Error) -> Self {
        ConrtollerError::IO_Error(value)
    }
}



impl fmt::Display for ConrtollerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConrtollerError::IO_Error(e) => {
                write!(f, "IO Error: {}", e)
            }
        }
    }
}


impl std::error::Error for ConrtollerError {}