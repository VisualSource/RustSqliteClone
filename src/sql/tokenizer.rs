use super::error::Error;

#[macro_export]
macro_rules! token {
    ($value:tt) => {
        crate::sql::tokenizer::Token::from($value)
    };
}

#[derive(Debug, PartialEq)]
pub enum Token {
    Comma,
    SemiComma,
    RightPren,
    LeftPren,
    Period,
    Star,
    String(String),
    Number(String),
    Ident(String),
    EOL,
}

impl Token {
    pub fn is_keyword(&self, keyword: &'static str) -> bool {
        match &self {
            Self::Ident(value) => value.to_lowercase() == keyword,
            _ => false,
        }
    }

    pub fn is_token(&self, token: &Token) -> bool {
        self == token
    }

    pub fn get_identifer(&self) -> Option<String> {
        match self {
            Self::Ident(value) => Some(value.to_owned().to_lowercase()),
            _ => None,
        }
    }
}

impl From<&str> for Token {
    fn from(value: &str) -> Self {
        match value {
            "(" => Token::LeftPren,
            ")" => Token::RightPren,
            "*" => Token::Star,
            ";" => Token::SemiComma,
            "," => Token::Comma,
            "." => Token::Period,
            _ => Token::Ident(value.into()),
        }
    }
}

impl From<String> for Token {
    fn from(value: String) -> Self {
        match value.as_str() {
            "(" => Token::LeftPren,
            ")" => Token::RightPren,
            "*" => Token::Star,
            ";" => Token::SemiComma,
            "," => Token::Comma,
            "." => Token::Period,
            _ => Token::Ident(value),
        }
    }
}

impl From<char> for Token {
    fn from(value: char) -> Self {
        match value {
            ')' => Self::RightPren,
            '(' => Self::LeftPren,
            ',' => Self::Comma,
            '*' => Self::Star,
            ';' => Self::SemiComma,
            _ => Self::EOL,
        }
    }
}

pub fn tokenizer(buffer: &String) -> Result<Vec<Token>, Error> {
    let mut input = buffer.chars().peekable();

    let mut tokens = vec![];
    while let Some(char) = input.next() {
        match char {
            e if e.is_control() => continue,
            e if e.is_whitespace() => continue,
            e if e.is_numeric() => {
                let mut value = String::default();

                value.push(e);

                while let Some(item) = input.peek() {
                    if !item.is_numeric() {
                        break;
                    }

                    if let Some(c) = input.next() {
                        value.push(c);
                    }
                }

                tokens.push(Token::Number(value))
            }
            'a'..='z' | 'A'..='Z' | '_' => {
                let mut value = String::default();

                value.push(char);

                while let Some(item) = input.peek() {
                    if !item.is_alphabetic() {
                        break;
                    }
                    if let Some(c) = input.next() {
                        value.push(c);
                    }
                }

                tokens.push(token!(value));
            }
            '\"' => {
                let mut value = String::default();
                while let Some(item) = input.peek() {
                    if item == &'\"' {
                        input.next();
                        break;
                    }

                    if let Some(c) = input.next() {
                        value.push(c);
                    }
                }

                tokens.push(Token::String(value))
            }
            '(' | ')' | '.' | '*' | ',' | ';' => tokens.push(token!(char)),

            _ => {
                return Err(Error::UnknownChar(format!(
                    ": Unknown char: {}",
                    char.escape_debug()
                )))
            }
        }
    }

    Ok(tokens)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_parse() {
        let input = "insert into TABLE (id,username) values (1,\"tset-user\");".to_string();

        let tokens = super::tokenizer(&input);

        println!("{:#?}", tokens);
    }
}
