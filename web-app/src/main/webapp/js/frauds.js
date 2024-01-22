const fraudEventSocket = new WebSocket('ws://localhost:8080/web-app/events/frauds');


fraudEventSocket.onmessage = function (event) {
    const eventData = JSON.parse(event.data);
    const tableBody = document.querySelector("#eventTable tbody");

    let newRow = document.createElement("tr");
    newRow.innerHTML = "<td>" + eventData.timestamp + "</td>" +
        "<td>" + eventData.customerId + "</td>" +
        "<td>" + eventData.fraudType + "</td>";

    tableBody.appendChild(newRow);
};