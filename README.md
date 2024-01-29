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


## Technologies

1. Docker.

2. MySQL (to support login, vehicles and logging system).

3. Apache Kafka (3 topics: vehicles, transactions and frauds).
