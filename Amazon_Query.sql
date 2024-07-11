Create database Amazon
use Amazon 
----
Create table Segment 
(
Segment varchar(100),
SegmentID varchar(50) not null primary key
)

Create table OrderPriority
(
OrderPriority varchar(100),
OrderPriorityID varchar(50) not null primary key
)

Create table SubCategory
(
SubCategory varchar(100),
SubCategoryID varchar(50) not null primary key
)

Create table Product
(
ProductName varchar(100),
ProductID varchar(50) not null primary key
)

Create table Category
(
Category varchar(100),
CategoryID varchar(50) not null primary key
)
Create table Region
(
Region varchar(100),
RegionID varchar(50) not null primary key,
)

Create table Market
(
Market varchar(100),
MarketID varchar(50) not null primary key
)

Create table Country
(
Country varchar(100),
CountryID varchar(50) not null primary key,
)

Create table State
(
State varchar(100),
StateID varchar(50) not null primary key,
)

Create table City
(
City varchar(100),
CityID varchar(50) not null primary key,
)

Create table ShipMode
(
ShipMode varchar(100),
ShipModeID varchar(50) not null primary key
)

Create table Customer
(
CustomerName varchar(100),
CustomerID varchar(50) not null primary key,
)

---

Create table Fact 
(
RowID varchar(50) not null primary key,
OrderID varchar(100), 
OrderDate date,
ShipDate date,
CustomerID varchar(50) not null,
ProductID varchar(50) not null,
ShipModeID varchar(50) not null,
SegmentID varchar(50) not null,
CityID varchar(50) not null,
StateID varchar(50) not null,
CountryID varchar(50) not null,
RegionID varchar(50) not null,
MarketID varchar(50) not null,
CategoryID varchar(50) not null,
SubCategoryID varchar(50) not null,
OrderPriorityID varchar(50) not null,
Sales float,
Quantity int,
Discount float,
Profit float,
ShippingCost float,
Constraint FK_Cus foreign key (CustomerID) references Customer(CustomerID),
Constraint FK_Pro foreign key (ProductID) references Product(ProductID),
Constraint FK_Shipm foreign key (ShipModeID) references ShipMode(ShipModeID),
Constraint FK_Seg foreign key (SegmentID) references Segment(SegmentID),
Constraint FK_Ci foreign key (CityID) references City(CityID),
Constraint FK_Re foreign key (RegionID) references Region(RegionID),
Constraint FK_Ma foreign key (MarketID) references Market(MarketID),
Constraint FK_Ca foreign key (CategoryID) references Category(CategoryID),
Constraint FK_Sub foreign key (SubCategoryID) references SubCategory(SubCategoryID),
Constraint FK_Pr foreign key (OrderPriorityID) references OrderPriority(OrderPriorityID),
Constraint FK_Sta foreign key (StateID) references State(StateID),
Constraint FK_Cou foreign key (CountryID) references Country(CountryID)
)

-- Trigger
-- 1. Create a trigger to calculate total revenue when there is a new order

CREATE TRIGGER update_total_sales
AFTER INSERT ON Fact
FOR EACH ROW
BEGIN
    DECLARE total_sales FLOAT;
    SELECT SUM(Sales)
    INTO total_sales
    FROM Fact
    WHERE CustomerID = NEW.CustomerID;

    UPDATE Customer
    SET TotalSales = total_sales
    WHERE CustomerID = NEW.CustomerID;
END 

-- 2. Create a discount check trigger

CREATE TRIGGER check_discount_threshold
BEFORE INSERT ON Fact
FOR EACH ROW
BEGIN
    IF NEW.Discount > 0.2 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Discount exceeds threshold. Please reduce discount.';
    END IF;
END 

-- 3. Create a trigger to calculate total profit

CREATE TRIGGER update_total_profit
AFTER UPDATE ON Fact
FOR EACH ROW
BEGIN
    DECLARE total_profit FLOAT;
    SELECT SUM(Profit)
    INTO total_profit
    FROM Fact
    WHERE OrderID = NEW.OrderID;

    UPDATE Fact
    SET TotalProfit = total_profit
    WHERE OrderID = NEW.OrderID;
END 

-- 4. Create a trigger to prevent deletion of customer information

CREATE TRIGGER prevent_delete_customer
BEFORE DELETE ON Customer
FOR EACH ROW
BEGIN
    DECLARE num_orders INT;
    SELECT COUNT(*)
    INTO num_orders
    FROM Fact
    WHERE CustomerID = OLD.CustomerID;

    IF num_orders > 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Customer cannot be deleted as there are orders associated with this customer.';
    END IF;
END 

-- 5. Create triggers to ensure data consistency

CREATE TRIGGER maintain_data_consistency_customer
AFTER INSERT, UPDATE, DELETE ON Customer
FOR EACH ROW
BEGIN
    IF INSERTING THEN

    ELSEIF UPDATING THEN

    ELSEIF DELETING THEN

    END IF;
END 

-- Procedure
-- 1. Procedure to calculate total profit for each customer over a period of time

CREATE PROCEDURE CalculateCustomerProfitInRange(
    IN start_date DATE,
    IN end_date DATE
)
BEGIN
    SELECT Customer.CustomerID, Customer.CustomerName, SUM(Fact.Profit) AS TotalProfit
    FROM Customer
    JOIN Fact ON Customer.CustomerID = Fact.CustomerID
    WHERE Fact.OrderDate BETWEEN start_date AND end_date
    GROUP BY Customer.CustomerID, Customer.CustomerName
    ORDER BY TotalProfit DESC;
END 

-- 3. Procedure adds new customers and corresponding orders

CREATE PROCEDURE AddCustomerWithOrders(
    IN customer_name VARCHAR(100),
    IN product_ids VARCHAR(255), 
    IN quantities VARCHAR(255)  
)
BEGIN
    DECLARE new_customer_id VARCHAR(50);
    DECLARE product_id VARCHAR(50);
    DECLARE quantity INT;
    
    INSERT INTO Customer (CustomerName) VALUES (customer_name);
    SET new_customer_id = LAST_INSERT_ID();
    
    SET @product_ids = product_ids;
    SET @quantities = quantities;
    
    WHILE CHAR_LENGTH(@product_ids) > 0 DO
        SET @product_id = SUBSTRING_INDEX(@product_ids, ',', 1);
        SET @quantity = SUBSTRING_INDEX(@quantities, ',', 1);
        
        INSERT INTO Fact (CustomerID, ProductID, Quantity, OrderDate)
        VALUES (new_customer_id, @product_id, @quantity, CURDATE());
        
        SET @product_ids = TRIM(BOTH ',' FROM SUBSTRING(@product_ids, CHAR_LENGTH(@product_id) + 2));
        SET @quantities = TRIM(BOTH ',' FROM SUBSTRING(@quantities, CHAR_LENGTH(@quantity) + 2));
    END WHILE;
    
    SELECT CONCAT('Added customer with ID ', new_customer_id) AS Message;
END 

-- 3. Procedure for updating profits for orders according to the new discount level

CREATE PROCEDURE UpdateProfitWithDiscount(
    IN new_discount FLOAT
)
BEGIN
    UPDATE Fact
    SET Profit = Sales * (1 - new_discount) - ShippingCost
    WHERE Discount <> new_discount;

    SELECT CONCAT('Updated profits with new discount: ', new_discount) AS Message;
END 

-- 4. Procedure to delete orders older than a number of days for a specific customer


CREATE PROCEDURE DeleteOldOrdersForCustomer(
    IN customer_id VARCHAR(50),
    IN days_old INT
)
BEGIN
    DELETE FROM Fact
    WHERE CustomerID = customer_id
    AND OrderDate < DATE_SUB(CURDATE(), INTERVAL days_old DAY);

    SELECT CONCAT('Deleted orders older than ', days_old, ' days for customer ', customer_id) AS Message;
END 

-- 5. Procedure to check the total number of products sold by each product category

CREATE PROCEDURE CalculateTotalQuantityByCategory()
BEGIN
    SELECT Category.Category, SUM(Fact.Quantity) AS TotalQuantity
    FROM Fact
    JOIN Product ON Fact.ProductID = Product.ProductID
    JOIN Category ON Product.CategoryID = Category.CategoryID
    GROUP BY Category.Category
    ORDER BY TotalQuantity DESC;
END 

-- Query
-- 1. Query to calculate total sales for each customer in the current year 

SELECT Customer.CustomerID, Customer.CustomerName, SUM(Fact.Sales) AS TotalSales
FROM Customer
JOIN Fact ON Customer.CustomerID = Fact.CustomerID
WHERE YEAR(Fact.OrderDate) = YEAR(CURDATE())
GROUP BY Customer.CustomerID, Customer.CustomerName
ORDER BY TotalSales DESC;

-- 2. Query to find the top-selling products in each category and subcategory 

SELECT Category.Category, SubCategory.SubCategory, Product.ProductName, SUM(Fact.Quantity) AS TotalQuantity
FROM Fact
JOIN Product ON Fact.ProductID = Product.ProductID
JOIN SubCategory ON Product.SubCategoryID = SubCategory.SubCategoryID
JOIN Category ON Product.CategoryID = Category.CategoryID
GROUP BY Category.Category, SubCategory.SubCategory
HAVING TotalQuantity = (
   SELECT MAX(TotalQuantity)
   FROM (
       SELECT SUM(Fact.Quantity) AS TotalQuantity
       FROM Fact
       JOIN Product ON Fact.ProductID = Product.ProductID
       JOIN SubCategory ON Product.SubCategoryID = SubCategory.SubCategoryID
       JOIN Category ON Product.CategoryID = Category.CategoryID
       GROUP BY Category.Category, SubCategory.SubCategory
   ) AS MaxQuantity
);

-- 3. Query to calculate total profit by region and market 

SELECT Region.Region, Market.Market, SUM(Fact.Profit) AS TotalProfit
FROM Fact
JOIN Region ON Fact.RegionID = Region.RegionID
JOIN Market ON Fact.MarketID = Market.MarketID
GROUP BY Region.Region, Market.Market
ORDER BY TotalProfit DESC;

-- 4. Query to count orders shipped by each shipping mode in the current month 

SELECT ShipMode.ShipMode, COUNT(Fact.OrderID) AS TotalOrders
FROM Fact
JOIN ShipMode ON Fact.ShipModeID = ShipMode.ShipModeID
WHERE MONTH(Fact.ShipDate) = MONTH(CURDATE()) AND YEAR(Fact.ShipDate) = YEAR(CURDATE())
GROUP BY ShipMode.ShipMode
ORDER BY TotalOrders DESC;

-- 5. Query to calculate total quantity sold and total sales by product category 
SELECT Category.Category, COUNT(Fact.Quantity) AS TotalQuantity, SUM(Fact.Sales) AS TotalSales
FROM Fact
JOIN Product ON Fact.ProductID = Product.ProductID
JOIN Category ON Product.CategoryID = Category.CategoryID
GROUP BY Category.Category
ORDER BY TotalSales DESC;













