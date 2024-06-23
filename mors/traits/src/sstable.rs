pub trait Table: Sized {
    type ErrorType;
    type TableBuilder: TableBuilder;
}
pub trait TableBuilder: Default {
    
}