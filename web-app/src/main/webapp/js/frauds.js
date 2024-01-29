const host = document.location.host;
const fraudEventSocket = new WebSocket('ws://' + host + '/web-app/events/frauds');
const tableBody = document.querySelector("#eventTable tbody");

const MAX_ROWS = 10;

fraudEventSocket.onopen = function (event) {
    console.log("WebSocket OPENED (url='ws://" + host + "/web-app/events/frauds')");
};

fraudEventSocket.onmessage = function (event) {
    const eventData = JSON.parse(event.data);

    console.log(eventData);

    // Create a new row
    let newRow = document.createElement("tr");
    newRow.innerHTML =
        "<td>" + eventData.timestamp + "</td>" +
        "<td>" + eventData.customerId + "</td>" +
        "<td>" + eventData.fraudType + "</td>"+
        "<td>" + eventData.customer.name + "</td>" +
        "<td>" + eventData.customer.email + "</td>" +
        "<td>" + eventData.customer.country + "</td>";



    // Add the new row to the beginning of the table
    tableBody.insertBefore(newRow, tableBody.firstChild);

    // Remove the last row if the table exceeds the maximum number of rows
    if (tableBody.children.length > MAX_ROWS) {
        tableBody.removeChild(tableBody.lastChild);
    }
};