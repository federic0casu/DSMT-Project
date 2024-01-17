// Initialize and add the map
let map;

let markers = [];

async function initMap(callback) {
    const { Map } = await google.maps.importLibrary("maps");
    const { AdvancedMarkerElement } = await google.maps.importLibrary("marker");
    map = new Map(document.getElementById("map"), {
        center: { lat: 45.442998228, lng: 9.273665572 },
        zoom: 16,
        mapId: "5413b2dc4dc00c76",
    });
}

async function updateMapWithCarLocation(vin, latitude, longitude) {
    if (!map) {
        console.error("Map not initialized");
        return;
    }

    // Update the marker position on the map
    const position = new google.maps.LatLng(latitude, longitude);

    for (let i = 0; i < markers.length; i++) {
        if (vin === markers[i].vin) {
            markers[i].marker.position = position;
            return; // Exit the function if marker is updated
        }
    }

    // If no existing marker found, create a new one
    const vinTag = document.createElement("div");
    vinTag.className = "vin-tag";
    vinTag.textContent = vin;

    const { AdvancedMarkerElement } = await google.maps.importLibrary("marker");
    const marker = new AdvancedMarkerElement({
        map,
        position: position,
        content: vinTag,
    });

    markers.push({ vin, marker });
}

document.addEventListener("DOMContentLoaded", function () {
    initMap().then(r => {
        const host = document.location.host;
        const url = "ws://" + host + "/web_app/events/geo-localization";

        const geoLocalizationEventSocket = new WebSocket(url);

        geoLocalizationEventSocket.onopen = function (event) {
            console.log("WebSocket OPENED");
        }

        geoLocalizationEventSocket.onclose = function (event) {
            console.log("WebSocket CLOSED");
        }

        geoLocalizationEventSocket.onerror = function (error) {
            console.error("WebSocket ERROR", error);
        }

        geoLocalizationEventSocket.onmessage = function (event) {
            console.log("WebSocket MESSAGE");

            const location = JSON.parse(event.data);
            const vin = location['car']['vin'];
            const lat = location['lat']
            const lon = location['lon']

            console.log(`Received location data: VIN=${vin}, Latitude=${lat}, Longitude=${lon}`);

            // Update the map with the new car location
            updateMapWithCarLocation(vin, lat, lon).then(tmp =>
                console.log("Map UPDATED")
            );
        };
    })
});
