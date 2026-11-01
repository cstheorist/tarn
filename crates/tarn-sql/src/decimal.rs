//! Exact decimal identity and rounding. Never use a binary float for an
//! exact-value comparison, index probe, DISTINCT key or integer assignment.
use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct Decimal {
    negative: bool,
    digits: String,
    exponent: i64,
}

impl Decimal {
    pub(crate) fn canonical(&self) -> String {
        format!(
            "{}{}e{}",
            if self.negative { "-" } else { "" },
            self.digits,
            self.exponent
        )
    }
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let text = text.trim();
        let (negative, text) = match text.strip_prefix('-') {
            Some(t) => (true, t),
            None => (false, text.strip_prefix('+').unwrap_or(text)),
        };
        let (mantissa, exponent) = match text.find(['e', 'E']) {
            Some(i) => (&text[..i], text[i + 1..].parse::<i64>().ok()?),
            None => (text, 0),
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        if whole.is_empty() && fraction.is_empty() || !whole.bytes().chain(fraction.bytes()).all(|b| b.is_ascii_digit())
        {
            return None;
        }
        let digits = format!("{whole}{fraction}");
        let digits = digits.trim_start_matches('0');
        if digits.is_empty() {
            return Some(Self {
                negative: false,
                digits: "0".into(),
                exponent: 0,
            });
        }
        let normalized = digits.trim_end_matches('0');
        let exponent = exponent
            .checked_sub(i64::try_from(fraction.len()).ok()?)?
            .checked_add(i64::try_from(digits.len() - normalized.len()).ok()?)?;
        // Comparison uses this sum as the decimal point position.
        exponent.checked_add(i64::try_from(normalized.len()).ok()?)?;
        Some(Self {
            negative,
            digits: normalized.into(),
            exponent,
        })
    }

    pub(crate) fn compare(&self, other: &Self) -> Ordering {
        let zero = self.digits == "0";
        let other_zero = other.digits == "0";
        if zero || other_zero {
            return match (zero, other_zero) {
                (true, true) => Ordering::Equal,
                (true, false) if other.negative => Ordering::Greater,
                (false, true) if self.negative => Ordering::Less,
                (true, false) => Ordering::Less,
                _ => Ordering::Greater,
            };
        }
        if self.negative != other.negative {
            return other.negative.cmp(&self.negative);
        }
        let magnitude = (self.digits.len() as i64 + self.exponent)
            .cmp(&(other.digits.len() as i64 + other.exponent))
            .then_with(|| {
                let len = self.digits.len().max(other.digits.len());
                self.digits
                    .bytes()
                    .chain(std::iter::repeat(b'0'))
                    .take(len)
                    .cmp(other.digits.bytes().chain(std::iter::repeat(b'0')).take(len))
            });
        if self.negative {
            magnitude.reverse()
        } else {
            magnitude
        }
    }

    pub(crate) fn integer(&self) -> Option<i128> {
        if self.exponent < 0 || self.exponent > 38 {
            return None;
        }
        let value = self.digits.parse::<i128>().ok()?;
        let value = if self.negative { value.checked_neg()? } else { value };
        value.checked_mul(10i128.checked_pow(self.exponent as u32)?)
    }

    /// Round half away from zero without expanding an arbitrary exponent.
    /// SQL DECIMAL scales are at most 30; 65 also accommodates intermediate
    /// exact expressions. Refuse larger outputs instead of unbounded padding.
    pub(crate) fn rescale(&self, scale: u32) -> Option<String> {
        if scale > 65 {
            return None;
        }
        let shift = self.exponent.checked_add(scale as i64)?;
        let keep = (self.digits.len() as i64).checked_add(shift)?;
        if keep > 4096 {
            return None;
        }
        let mut digits = if keep <= 0 {
            Vec::new()
        } else {
            self.digits.as_bytes()[..(keep as usize).min(self.digits.len())].to_vec()
        };
        if shift >= 0 {
            digits.resize(keep as usize, b'0');
        } else if keep >= 0 && self.digits.as_bytes()[keep as usize] >= b'5' {
            let mut carry = true;
            for digit in digits.iter_mut().rev() {
                if *digit < b'9' {
                    *digit += 1;
                    carry = false;
                    break;
                }
                *digit = b'0';
            }
            if carry {
                digits.insert(0, b'1');
            }
        }
        if digits.len() <= scale as usize {
            let mut padded = vec![b'0'; scale as usize + 1 - digits.len()];
            padded.extend(digits);
            digits = padded;
        }
        let zero = digits.iter().all(|&b| b == b'0');
        if scale > 0 {
            digits.insert(digits.len() - scale as usize, b'.');
        }
        if self.negative && !zero {
            digits.insert(0, b'-');
        }
        String::from_utf8(digits).ok()
    }
}
