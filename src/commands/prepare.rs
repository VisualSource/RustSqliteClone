use super::Statement;
use crate::errors::DBError;
use crate::sql::{interpect, tokenizer};

pub fn prepare_statement(buffer: &String) -> DBError<Statement> {
    let tokens = tokenizer(buffer)?;

    let value = interpect(tokens)?;

    Ok(value)
}
