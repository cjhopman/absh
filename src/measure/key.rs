#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum MeasureKey {
    WallTime,
    MaxRss,
    User(usize),
}

impl MeasureKey {
    pub fn index(&self) -> usize {
        match self {
            MeasureKey::WallTime => 0,
            MeasureKey::MaxRss => 1,
            MeasureKey::User(u) => 2 + u,
        }
    }

    pub fn from_index(index: usize) -> Self {
        match index {
            0 => MeasureKey::WallTime,
            1 => MeasureKey::MaxRss,
            u => MeasureKey::User(u - 2),
        }
    }
}
