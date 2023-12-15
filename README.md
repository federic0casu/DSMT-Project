# Fraud Detection Project


## Use cases

Actor: **Admin**

The system must allow an *Admin* to:

- UC1: Login into the web-based dashboard by providing his/her username and password.

- UC2: Logout.

- UC3: See the current location of the running vehicles as a point in a map.

- UC4: Select a vehicle:

	- UC4.1: View informations about the current state of the vehicle (e.g. vin, speed, current mileage, ...)

	- UC4.2: View if there are any fraud alerts.

- UC5: View a table-like dashboard displaying a report of the current state of the fleet.

	- UC5.1: For each vehicle display its current state (rented, mileage, status, possible frauds, ...)


## Architecture

1. Spring Boot (to support the web-based dashboard).

2. MySQL (to support login, vehicles and logging system).

3. Apache Kafka (3 topics: vehicles, transactions and frauds).
	
4. Erlang Logging System: each Kafka Consumer will have its own Erlang node used to manage a distributed logging system. Whenever a Kafka consumer detects a fraud or a system fault (e.g., communication problem, missing data, ...), it will send a message to an Erlang Server (from now on Erlang Dispatcher). The Erlang Dispatcher decides to which replica of the database to send the log message. Optional: we'd like to implement a distributed algorithm to manage the consistency of the distributed database.
