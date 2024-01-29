const host = document.location.host;
const violationEventSocket = new WebSocket('ws://' + host + '/web-app/events/violations');
const violationsTableBody = document.querySelector("#speedingViolationsTable tbody");

const MAX_ROWS = 10;

violationEventSocket.onopen = function (event) {
    console.log("WebSocket OPENED (url='ws://" + host + "/web-app/events/violations')");
};

violationEventSocket.onmessage = function (event) {
    const violationData = JSON.parse(event.data);

    console.log(violationData);

    // Format the date
    const date = new Date(violationData.violationTs);
    const formattedDate = date.getFullYear() + "-" +
        String(date.getMonth() + 1).padStart(2, '0') + "-" +
        String(date.getDate()).padStart(2, '0') + " " +
        String(date.getHours()).padStart(2, '0') + ":" +
        String(date.getMinutes()).padStart(2, '0') + ":" +
        String(date.getSeconds()).padStart(2, '0');

    // Create a new row for the violation data
    let newRow = document.createElement("tr");
    newRow.innerHTML =
        "<td>" + violationData.vin + "</td>" +
        "<td>" + formattedDate + "</td>" +
        "<td>" + violationData.userSpeed + " km/h" + "</td>" +
        "<td>" + (violationData.speedLimit ? violationData.speedLimit.maxSpeed + " km/h" : "N/A") + "</td>" +
        "<td>" + (violationData.speedLimit ? violationData.speedLimit.wayName : "N/A") + "</td>";

    // Add the new row to the beginning of the table
    violationsTableBody.insertBefore(newRow, violationsTableBody.firstChild);

    // Remove the last row if the table exceeds the maximum number of rows
    if (violationsTableBody.children.length > MAX_ROWS) {
        violationsTableBody.removeChild(violationsTableBody.lastChild);
    }
};
