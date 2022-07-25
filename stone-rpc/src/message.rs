use std::io::{self, Read};
use rmpv::{decode::{self, value}, encode, Integer, Utf8String, Value};
use super::error::DecodeError;

#[derive(PartialEq, Clone, Debug)]
pub enum Message {
    Request(Request),
    Response(Response)
}

const REQUEST_MESSAGE: u64 = 0;
const RESPONSE_MESSAGE: u64 = 1;
const REQUEST_MESSAGE_SET:u64 = 2;
const REQUEST_MESSAGE_GET:u64 = 3;
const REQUEST_MESSAGE_SCAN:u64 = 4;
const RESPONSE_MESSAGE_SUCCESS:u64 = 5;
const RESPONSE_MESSAGE_FAILURE:u64 = 6;

impl Message {
    pub fn decode<R: Read>(rd: &mut R) -> Result<Message,DecodeError> {
        let msg = value::read_value(rd)?;
        if let Value::Array(ref array) = msg {
            if array.len() != 3 {
                return Err(DecodeError::Invalid);
            }
            if let Value::Integer(msg_type) = array[0] {
                match msg_type.as_u64() {
                    Some(REQUEST_MESSAGE) => {Ok(Message::Request(Request::decode(array)?))},
                    Some(RESPONSE_MESSAGE) => {Ok(Message::Response(Response::decode(array)?))},
                    _ => {return Err(DecodeError::Invalid);}
                }
            }
            else{
                return Err(DecodeError::Invalid);
            }
        }
        else{
            return Err(DecodeError::Invalid);
        }
    }

    pub fn as_value(&self) -> Value {
        match *self {
            Message::Request(Request{id, ref command}) => {
                let cmd = match command {
                    Command::Set(key, value) => {
                        vec![
                            Value::Integer(Integer::from(REQUEST_MESSAGE_SET)),
                            key.clone(),
                            value.clone()
                        ]
                    },
                    Command::Get(key) => {
                        vec![
                            Value::Integer(Integer::from(REQUEST_MESSAGE_GET)),
                            key.clone(),
                        ]
                    },
                    Command::Scan(start, end) => {
                        vec![
                            Value::Integer(Integer::from(REQUEST_MESSAGE_SCAN)),
                            start.clone(),
                            end.clone()
                        ]
                    },
                };
                Value::Array(vec![
                    Value::Integer(Integer::from(REQUEST_MESSAGE)),
                    Value::Integer(Integer::from(id)),
                    Value::Array(cmd)
                ])
            },
            Message::Response(Response{id, ref notification}) => {
                let notf = match notification {
                    Notification::Success(result) => {
                        vec![
                            Value::Integer(Integer::from(RESPONSE_MESSAGE_SUCCESS)),
                            result.clone(),
                        ]
                    },
                    Notification::Failure(msg) => {
                        vec![
                            Value::Integer(Integer::from(RESPONSE_MESSAGE_FAILURE)),
                            Value::String(Utf8String::from(msg.as_str()))
                        ]
                    }
                };
                Value::Array(vec![
                    Value::Integer(Integer::from(RESPONSE_MESSAGE)),
                    Value::Integer(Integer::from(id)),
                    Value::Array(notf)
                ])
            }
        }
    }

    pub fn pack(&self) -> io::Result<Vec<u8>> {
        let mut bytes = vec![];
        encode::write_value(&mut bytes, &self.as_value())?;
        Ok(bytes)
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct Request {
    id: u64,
    command: Command
}

#[derive(PartialEq, Clone, Debug)]
pub struct Response {
    id: u64,
    notification: Notification
}

#[derive(PartialEq, Clone, Debug)]
pub enum Command {
    Set(Value, Value),
    Get(Value),
    Scan(Value, Value)
} 

#[derive(PartialEq, Clone, Debug)]
pub enum Notification {
    Success(Value),
    Failure(String)
}

impl Request {
    fn decode(array: &[Value]) -> Result<Self, DecodeError> {
        let id = if let Value::Integer(id) = array[1] {
            id.as_u64().unwrap()
        } else {
            return Err(DecodeError::Invalid);
        };

        let command = if let Value::Array(items) = &array[2] {
            if let Value::Integer(cmd_type) = items[0] {
                match cmd_type.as_u64() {
                    Some(REQUEST_MESSAGE_SET) => {Command::Set(items[1].clone(), items[2].clone())},
                    Some(REQUEST_MESSAGE_GET) => {Command::Get(items[1].clone())},
                    Some(REQUEST_MESSAGE_SCAN) => {Command::Scan(items[1].clone(), items[2].clone())},
                    _ => return Err(DecodeError::Invalid)
                }
            }else {
                return Err(DecodeError::Invalid);
            }
        } else {
            return Err(DecodeError::Invalid);
        };
        Ok(Request{ id, command })
    }
}

impl Response {
    fn decode(array: &[Value]) -> Result<Self, DecodeError> {
        let id = if let Value::Integer(id) = array[1] {
            id.as_u64().unwrap()               
        } else {
            return Err(DecodeError::Invalid);
        };

        let notification = if let Value::Array(items) = &array[2] {
            if let Value::Integer(notf_type) = items[0] {
                match notf_type.as_u64() {
                    Some(RESPONSE_MESSAGE_SUCCESS) =>{Notification::Success(items[1].clone())},
                    Some(RESPONSE_MESSAGE_FAILURE) =>{Notification::Failure(items[1].to_string())},
                    _ => return Err(DecodeError::Invalid)
                }
            }
            else{
                return Err(DecodeError::Invalid);
            }
        }else {
            return Err(DecodeError::Invalid);
        }; 
        Ok(Response{id, notification})
    }
}