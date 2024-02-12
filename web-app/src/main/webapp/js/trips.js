let url = "ws://" + host + "/web-app/events/trip-reports";

const tripReportsSocket = new WebSocket(url);

tripReportsSocket.onmessage = function (event) {
    const carData = JSON.parse(event.data);

    // Update the car table
    updateCarTable(carData);
};

function updateCarTable(carData) {
    const carTableBody = document.querySelector("#carTable tbody");

    // Check if a row with the same VIN already exists in the table
    const existingRow = document.querySelector(`#carTable tbody tr[data-vin="${carData.car.vin}"]`);

    if (existingRow) {
        // Update values in the existing row
        existingRow.innerHTML = `<td>${carData.car.vin}</td>` +
            `<td>${carData.car.manufacturer}</td>` +
            `<td>${carData.car.model}</td>` +
            `<td>${carData.car.fuelType}</td>` +
            `<td>${carData.car.color}</td>` +
            `<td>${carData.consumption}</td>` +
            `<td>${carData.distance}</td>`;
    } else {
        // Create a new row
        let newRow = document.createElement("tr");
        newRow.setAttribute("data-vin", carData.car.vin);
        newRow.innerHTML = `<td>${carData.car.vin}</td>` +
            `<td>${carData.car.manufacturer}</td>` +
            `<td>${carData.car.model}</td>` +
            `<td>${carData.car.fuelType}</td>` +
            `<td>${carData.car.color}</td>` +
            `<td>${carData.consumption}</td>` +
            `<td>${carData.distance}</td>`;

        // Add the new row to the beginning of the table
        carTableBody.insertBefore(newRow, carTableBody.firstChild);
    }
}