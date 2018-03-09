DROP table BXP_SYNCProductos;

CREATE TABLE BXP_SYNCProductos (
	DocEntry int,
	
	ItemCode varchar(20) not null,
	
	ItemName varchar(100) not null,
	
	logInstance INT

)
