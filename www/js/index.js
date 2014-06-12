var map;
var markers = [];
var geohashCells = [];

function clearMarkers() {
    while(markers.length){
        markers.pop().setMap(null);
    }
}

function addMarker(lat, lon, title, icon) {
    markers.push(new google.maps.Marker({

		position: new google.maps.LatLng(lat, lon),
        map: map,
        title: title,
        icon: icon,
        shadow: null
    }));
}

function clearGeohashCells() {
    while(geohashCells.length){
        geohashCells.pop().setMap(null);
    }
}

function addGeohashCell(geohashCell) {
    geohashCells.push(new google.maps.Rectangle({

		strokeColor: '#FF0000',
        strokeOpacity: 0.8,
        strokeWeight: 2,
        fillColor: '#FF0000',
        fillOpacity: 0.0,
        map: map,
        bounds: new google.maps.LatLngBounds(
            new google.maps.LatLng(geohashCell.top_left.lat, geohashCell.top_left.lon),
            new google.maps.LatLng(geohashCell.bottom_right.lat, geohashCell.bottom_right.lon))
    }));
}

function fetchFacets(query) {

	var elasticSearchQuery = prepareElasticSearchQuery(query);

    $.ajax({

        url: "http://localhost:9200/twitter/_search?search_type=count",
        contentType: "text/json",
        type: "POST",
        data: JSON.stringify(elasticSearchQuery),
        dataType: "json"

	}).done(function(data){

        clearMarkers();
        clearGeohashCells();

        var clusters = data.facets.places.clusters;
         console.log('received ' + clusters.length + ' clusters');

        for (var i = 0; i < clusters.length; i++) {

            addMarker(
                    clusters[i].center.lat,
                    clusters[i].center.lon,
                    clusters[i].total == 1?
                        "item desc @" + clusters[i].center.lat + ", " + clusters[i].center.lon:
                        "cluster (" + clusters[i].total + ") @" + clusters[i].center.lat + ", " + clusters[i].center.lon,
                    groupIcon(clusters[i].total)
            );
             addGeohashCell(clusters[i].geohash_cell);
        }
    });
}

function prepareElasticSearchQuery(query) {
	var ne = map.getBounds().getNorthEast();
	var sw = map.getBounds().getSouthWest();
	var zoom = map.getZoom();
	var matchQuery;
	var factor = -0.04*zoom + 1.05;

	if(query !== undefined) {
		matchQuery = {
			match : {
				_all : query
			}
		}
	}else {
		matchQuery = {
			match_all : { }
		}
	}

	return {
		query: {
			filtered: {
				query: matchQuery,
				filter: {
					geo_bounding_box: {
						location: {
							top_left: {
								"lat": ne.lat(),
								"lon": sw.lng()
							},
							bottom_right: {
								"lat": sw.lat(),
								"lon": ne.lng()
							}
						}
					}
				}
			}
		},
		facets: {
			places: {
				geohash: {
					field: "location",
					factor: factor,
					show_geohash_cell: true
				}
			}
		}
	};
}

function groupIcon(groupSize) {
    return groupSize > 1?
        'https://chart.googleapis.com/chart?chst=d_map_spin&chld=1.0|0|FF8429|16|b|' + groupSize:
        'https://chart.googleapis.com/chart?chst=d_map_spin&chld=0.5|0|FF8429|16|b|';
}

function initMap(divId){
    var mapOptions = {
        zoom: 3,
        center: new google.maps.LatLng(50, 0),
        mapTypeId: google.maps.MapTypeId.ROADMAP
    };

    map = new google.maps.Map(document.getElementById(divId), mapOptions);

    //does it have sense while resizing?
    google.maps.event.addDomListener(window, 'resize', function(){ fetchFacets(); } );
    google.maps.event.addListener(map, 'dragend', function(){ fetchFacets(); } );
    google.maps.event.addListener(map, 'zoom_changed', function(){ fetchFacets(); } );
    google.maps.event.addListenerOnce(map, 'idle', function(){ fetchFacets(); });
	google.maps.event.addListener(document.getElementById("searchButton"), "click", function() {
		var query = $("#searchQuery").val();
		fetchFacets(query);
	});
}

$(document).ready(function() {
    initMap('map');
});

