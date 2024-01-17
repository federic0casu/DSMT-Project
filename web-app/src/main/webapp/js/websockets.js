const fraudEventSocket = new WebSocket('ws://localhost:8080/web_app/events/fraud');
fraudEventSocket.onmessage = function (event) {
    const eventData = JSON.parse(event.data);
    const tableBody = document.querySelector("#eventTable tbody");

    let newRow = document.createElement("tr");
    newRow.innerHTML = "<td>" + eventData.timestamp + "</td>" +
        "<td>" + eventData.severity + "</td>" +
        "<td>" + eventData.event + "</td>" +
        "<td>" + eventData.description + "</td>";

    tableBody.appendChild(newRow);
};