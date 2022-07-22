use std::io::{self, Read};
use rmpv::{decode::{self, value}, encode, Integer, Utf8String, Value};
use anyhow::Result;

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
    pub fn decode<R: Read>(rd: &mut R) -> Result<Message> {
        let msg = decode::value::read_value(rd)?;
        
        
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
}

#[derive(PartialEq, Clone, Debug)]
pub struct Request {
    id: u32,
    command: Command
}

#[derive(PartialEq, Clone, Debug)]
pub struct Response {
    id: u32,
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