use crate::errors::Error;
use crate::sql::Statement;
use crate::sql::{interperter::interpect, tokenizer::tokenizer};

pub fn prepare_statement(buffer: &String) -> Result<Statement, Error> {
    let tokens = tokenizer(buffer)?;

    let value = interpect(tokens)?;

    Ok(value)
}
