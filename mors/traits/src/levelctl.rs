pub trait LevelCtl: Sized {
    type ErrorType;
    type LevelCtlBuilder: LevelCtlBuilder;
}
pub trait LevelCtlBuilder: Default {}
