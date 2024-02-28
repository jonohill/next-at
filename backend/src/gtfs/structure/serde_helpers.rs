use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};

pub fn deserialize_option_unix_date<'de, D>(
    deserializer: D,
) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    let f_secs: Option<f64> = Deserialize::deserialize(deserializer)?;
    match f_secs {
        None => Ok(None),
        Some(secs) => {
            let secs = secs as i64;
            let dt = DateTime::<Utc>::from_timestamp(secs, 0)
                .ok_or_else(|| serde::de::Error::custom("Invalid timestamp"))?;
            Ok(Some(dt))
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Many<T> {
    /// Single value
    One(T),
    /// Array of values
    Many(Vec<T>),
}

// impl<T> Many<T> {
//     pub fn iter(&self) -> impl Iterator<Item = &T> {
//         match self {
//             Many::One(val) => return vec![val].iter(),
//             Many::Many(vec) => return *vec.iter(),
//         };
//     }
// }

impl<T> From<Many<T>> for Vec<T> {
    fn from(from: Many<T>) -> Self {
        match from {
            Many::One(val) => vec![val],
            Many::Many(vec) => vec,
        }
    }
}

impl<T> IntoIterator for Many<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        let vec: Vec<T> = self.into();
        vec.into_iter()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum MaybeStringWrapped<T> {
    Str(String),
    Val(T),
}

#[allow(unused)]
impl<T: FromStr> MaybeStringWrapped<T> {
    pub fn into_inner(self) -> Result<T, T::Err> {
        match self {
            MaybeStringWrapped::Str(s) => s.parse(),
            MaybeStringWrapped::Val(v) => Ok(v),
        }
    }
}
