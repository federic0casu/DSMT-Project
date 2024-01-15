const socket = new WebSocket('ws://127.0.0.1:8080/fraud-events');

socket.onmessage = function (event) {
    const eventData = JSON.parse(event.data);
    const tableBody = document.querySelector("#eventTable tbody");

    let newRow = document.createElement("tr");
    newRow.innerHTML = "<td>" + eventData.timestamp + "</td>" +
        "<td>" + eventData.severity + "</td>" +
        "<td>" + eventData.event + "</td>" +
        "<td>" + eventData.description + "</td>";

    tableBody.appendChild(newRow);
};