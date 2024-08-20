-- Create the vendor table
CREATE TABLE vendor (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- Insert into vendor table
INSERT INTO vendor (name)
SELECT DISTINCT vendor_id
FROM new_york_taxi;

-- Create the ratecode table
CREATE TABLE ratecode (
    id SERIAL PRIMARY KEY,
    description VARCHAR(255) NOT NULL
);

-- Insert into ratecode table
INSERT INTO ratecode (description)
SELECT DISTINCT ratecode
FROM new_york_taxi;

-- Create the payment_type table
CREATE TABLE payment_type (
    id SERIAL PRIMARY KEY,
    method VARCHAR(255) NOT NULL
);

-- Insert into payment_type table
INSERT INTO payment_type (method)
SELECT DISTINCT payment_type
FROM new_york_taxi;

-- Create the zone table
CREATE TABLE zone (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- Insert into zone table
INSERT INTO zone (name)
SELECT DISTINCT zone
FROM new_york_taxi;

-- Create the borough table
CREATE TABLE borough (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- Insert into borough table
INSERT INTO borough (name)
SELECT DISTINCT borough
FROM new_york_taxi;

-- Create the service_zone table
CREATE TABLE service_zone (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- Insert into service_zone table
INSERT INTO service_zone (name)
SELECT DISTINCT service_zone
FROM new_york_taxi;

-- Create the location table
CREATE TABLE location (
    id SERIAL PRIMARY KEY,
    zone_id INTEGER NOT NULL,
    borough_id INTEGER NOT NULL,
    service_zone_id INTEGER NOT NULL,
    FOREIGN KEY (zone_id) REFERENCES zone(id),
    FOREIGN KEY (borough_id) REFERENCES borough(id),
    FOREIGN KEY (service_zone_id) REFERENCES service_zone(id)
);

-- Insert into location table
INSERT INTO location (zone_id, borough_id, service_zone_id)
SELECT
    z.id AS zone_id,
    b.id AS borough_id,
    s.id AS service_zone_id
FROM new_york_taxi nt
JOIN zone z ON nt.zone = z.name
JOIN borough b ON nt.borough = b.name
JOIN service_zone s ON nt.service_zone = s.name;

-- Create the taxi_trip table
CREATE TABLE taxi_trip (
    id SERIAL PRIMARY KEY,
    vendor_id INTEGER NOT NULL,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    ratecode_id INTEGER NOT NULL,
    passenger_count INTEGER NOT NULL,
    trip_distance FLOAT NOT NULL,
    fare_amount FLOAT NOT NULL,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    ehail_fee FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT NOT NULL,
    payment_type_id INTEGER NOT NULL,
    trip_type VARCHAR(255),
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    location_id INTEGER NOT NULL,
    taxi_type VARCHAR(255),
    FOREIGN KEY (vendor_id) REFERENCES vendor(id),
    FOREIGN KEY (ratecode_id) REFERENCES ratecode(id),
    FOREIGN KEY (payment_type_id) REFERENCES payment_type(id),
    FOREIGN KEY (location_id) REFERENCES location(id)
);

-- Insert into taxi_trip table
INSERT INTO taxi_trip (
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    ratecode_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type_id,
    trip_type,
    congestion_surcharge,
    airport_fee,
    location_id,
    taxi_type
)
SELECT
    v.id AS vendor_id,
    nt.pickup_datetime,
    nt.dropoff_datetime,
    r.id AS ratecode_id,
    nt.passenger_count,
    nt.trip_distance,
    nt.fare_amount,
    nt.extra,
    nt.mta_tax,
    nt.tip_amount,
    nt.tolls_amount,
    nt.ehail_fee,
    nt.improvement_surcharge,
    nt.total_amount,
    pt.id AS payment_type_id,
    nt.trip_type,
    nt.congestion_surcharge,
    nt.airport_fee,
    l.id AS location_id,
    nt.taxi_type
FROM new_york_taxi nt
JOIN vendor v ON nt.vendor_id = v.name
JOIN ratecode r ON nt.ratecode = r.description
JOIN payment_type pt ON nt.payment_type = pt.method
JOIN location l ON nt.zone = (SELECT name FROM zone WHERE id = l.zone_id)
                  AND nt.borough = (SELECT name FROM borough WHERE id = l.borough_id)
                  AND nt.service_zone = (SELECT name FROM service_zone WHERE id = l.service_zone_id);