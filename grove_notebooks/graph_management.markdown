``` js
api = getApi()
```

``` js
data = Object()
```

``` js
md`# Graph Management`
```

``` js
Grove.Div({
  	className: "container",
	children: [
    	Grove.Div({
          className: "row",
          children: [
          	Grove.Div({
              className: "col text-center",
              children: [clearGraphBtn]	
            }),
            Grove.Div({
              className: "col text-center",
              children: [loadGraphBtn]	
            }),
            Grove.Div({
              className: "col text-center",
              children: [loadRoadBtn]	
            }),
            Grove.Div({
              className: "col text-center",
              children: []	
            }),
            Grove.Div({
              className: "col text-center",
              children: []	
            })
          ]
        })
    ]
})
```

``` js
Grove.Div({
  	className: "container",
	children: [
    	Grove.Div({
          className: "row",
          children: [
          	Grove.Div({
              className: "col text-center",
              children: [loadJunctionsBtn]	
            }),
            Grove.Div({
              className: "col text-center",
              children: [loadSegmentsBtn]	
            }),
            Grove.Div({
              className: "col text-center",
              children: [loadCrimesBtn]	
            }),
            Grove.Div({
              className: "col text-center",
              children: [loadTransitBtn]	
            }),
            Grove.Div({
              className: "col text-center",
              children: [deleteSelectionBtn]	
            })
          ]
        })
    ]
})
```

``` js
Grove.Div({
  	className: "container",
	children: [
    	Grove.Div({
          className: "row",
          children: [
          	Grove.Div({
              className: "col text-center",
              children: [selectJunctionsBtn]	
            }),
            Grove.Div({
              className: "col text-center",
              children: [selectSegmentsBtn]	
            }),
            Grove.Div({
              className: "col text-center",
              children: [selectCrimesBtn]	
            }),
            Grove.Div({
              className: "col text-center",
              children: [selectTransitBtn]	
            }),
            Grove.Div({
              className: "col text-center",
              children: [clearSelectionBtn]	
            })
          ]
        })
    ]
})
```

``` js
Grove.Div({
  	className: "container",
	children: [
    	Grove.Div({
          className: "row",
          children: [
          	Grove.Div({
              className: "col text-center",
              children: [expandCrimeBtn]	
            }),
            Grove.Div({
              className: "col text-center",
              children: [expandTransitBtn]	
            }),
            Grove.Div({
              className: "col text-center",
              children: [expandNetworkBtn]	
            }),
            Grove.Div({
              className: "col text-center",
              children: []	
            }),
            Grove.Div({
              className: "col text-center",
              children: []	
            })
          ]
        })
    ]
})
```

``` js
Grove.Div({
  	className: "container",
	children: [
    	Grove.Div({
          className: "row",
          children: [
          	Grove.Div({
              className: "col text-center",
              children: [toggleMapBtn]	
            }),
            Grove.Div({
              className: "col text-center",
              children: [loadSelectedRegionBtn]	
            }),
            Grove.Div({
              className: "col text-center", 
              children: [removeSelectedRegionBtn]	
            }),
            Grove.Div({
              className: "col text-center", 
              children: [clearRegionBtn]	
            }),
            Grove.Div({
              className: "col text-center", 
              children: [toggleShowSelectedBtn]	
            })
          ]
        })
    ]
})
```

``` js

```

``` js
{
  // Create the container for the map
  const container = data.map_container = yield htl.html`<div style="height: 600px;">`;
  
  const vancouver_pos = [49.25, -123];
  
  // Create the map
  const map = data.map = L.map(container).setView(vancouver_pos, 12);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "© <a href=https://www.openstreetmap.org/copyright>OpenStreetMap</a> contributors"
  }).addTo(map);
  
  // Create a layer to display selected nodes on
  let selectedNodesLayer = data.selectedNodesLayer = L.layerGroup().addTo(map);
  
  // Initialize the corners of the region
  let positions = data.positions = {lat1: 0, lng1: 0, lat2: 0, lng2: 0};
  
  // Create the markers for showing the region
  let marker1 = L.marker({interactive: false});
  let marker2 = L.marker({interactive: false});
  let polygon = L.polygon([], {interactive: false});
  
  // Handle clicks on the map
  //
  // Update the corners of the region selection
  function onMapClick(e) {
    // Determine which corner should be changed
    let first = !e.originalEvent.shiftKey;
    if (first) {
      // Update the position of the first corner
      ({ lat: positions.lat1, lng: positions.lng1 } = e.latlng);
      marker1.setLatLng(e.latlng).addTo(map);
      
      // If the second corner hasn't been set then update it so that no region shows
      if(!map.hasLayer(marker2)) {
       	({ lat: positions.lat2, lng: positions.lng2 } = e.latlng);
      }
    } else {
      // Update the positions of the second corner
      ({ lat: positions.lat2, lng: positions.lng2 } = e.latlng);
      marker2.setLatLng(e.latlng).addTo(map);
      
      // If the first corner hasn't been set then update it so that no region shows
      if(!map.hasLayer(marker1)) {
       	({ lat: positions.lat1, lng: positions.lng1 } = e.latlng);
      }
    }
    
    // Update the region display
    let corners = [
      [positions.lat1, positions.lng1], 
      [positions.lat1, positions.lng2],
      [positions.lat2, positions.lng2],
      [positions.lat2, positions.lng1]
    ];
    polygon.setLatLngs(corners).addTo(map);
  }
  map.on('click', onMapClick);
  
  // Reset the region selection to an empty selection
  function clearSelection() {
    // Reset all positions
    positions.lat1 = 0;
    positions.lat2 = 0;
    positions.lng1 = 0;
    positions.lng2 = 0;
    
    // Remove the displays from the map
    marker1.remove();
    marker2.remove();
    polygon.remove();
  }
  data.clearSelection = clearSelection;
  
  data.showSelected = true;
  
  // Show the selected nodes on the map if data.showSelected is true
  async function updateSelectedDisplay() {
    // Remove any previous markers
    data.selectedNodesLayer.clearLayers();
    
    if (!data.showSelected) return;
    
    let colors = {
      default: "black",
      Junction: "orange",
      Segment: "grey",
      Crime: "red",
      Transit: "blue"
    }
    
    let graph = api.getLayoutGraph();
    let selectedNodes = graph.getVisibleNodes().filter(filterBySelection());
    
    // Create a marker for each node
    selectedNodes.forEach((node) => {
      let lat = node.properties.latitude;
      let lng = node.properties.longitude;
      
      // Determine the color for the marker
      let color = colors.default;
      if (node.category in colors)
        color = colors[node.category]
      
      // Create the marker
      data.selectedNodesLayer.addLayer(L.circleMarker([lat, lng], {color: color, fillOpacity: 0.8, radius: 5}));
    })
  }
  data.updateSelectedDisplay = updateSelectedDisplay;
  api.observe("select", updateSelectedDisplay);
}
```

``` js
<style>
  .customBtn { 
    width: 200px;
    height: 70px;
  }
</style>
```

``` js
// Removes all nodes when clicked
clearGraphBtn = Grove.Button({
  label: "Clear Graph",
  className: "btn-primary btn-lg customBtn",
  onClick: () => {
    api.getLayoutGraph().clear();
    data.updateSelectedDisplay();
  }
})
```

``` js
// Loads all nodes from Neo4j when clicked
loadGraphBtn = Grove.Button({
  label: "Load Entire Graph",
  className: "btn-warning btn-lg customBtn",
  onClick: () => {
    api.neo4j(
      `MATCH (n) RETURN *`
    )
  }
})
```

``` js
// Loads the entire road network from Neo4j when clicked
loadRoadBtn = Grove.Button({
  label: "Load Entire Road Network",
  className: "btn-warning btn-lg customBtn",
  onClick: () => {
    api.neo4j(
      `MATCH (s:Segment)-[c:CONTINUES_TO]->(j:Junction) RETURN *`
    )
  }
})
```

``` js
// Shows and hides the map display when clicked
toggleMapBtn = Grove.Button({
  label: "Toggle Map",
  className: "btn-primary btn-lg customBtn",
  onClick: () => {
    let mapStyle = data.map_container.style;
    mapStyle.display = mapStyle.display == "" ? "none" : "" ;
  }
})
```

``` js
// Removes all nodes in the selected region when clicked
removeSelectedRegionBtn = Grove.Button({
  label: "Remove Selected Region",
  className: "btn-primary btn-lg customBtn",
  onClick: () => {
    // Remove nodes in the region
    api.getLayoutGraph().applyTransform((graph) => graph.removeNodes(
      graph.getVisibleNodes().filter(filterByRegion()).map((node) => node.id)
    ));
    
    // Update the display
    data.updateSelectedDisplay();
  }
})
```

``` js
// Load the road network in the selected region
loadSelectedRegionBtn = Grove.AsyncButton({
  label: "Load Selected Region",
  className: "btn-primary btn-lg customBtn",
  onClick: () => loadNodes("(s:Segment)-[c:CONTINUES_TO]->(j:Junction)", "j")
})
```

``` js
// Load junctions in the selected region
loadJunctionsBtn = Grove.Button({
  label: "Load Junctions",
  className: "btn-primary btn-lg customBtn",
  onClick: () => loadNodes("(j:Junction)", "j") 
})
```

``` js
// Load segments in the selected region
loadSegmentsBtn = Grove.Button({
  label: "Load Segments",
  className: "btn-primary btn-lg customBtn",
  onClick: () => loadNodes("(s:Segment)", "s") 
})
```

``` js
// Load crime nodes in the selected region
loadCrimesBtn = Grove.Button({
  label: "Load Crimes",
  className: "btn-primary btn-lg customBtn",
  onClick: () => loadNodes("(c:Crime)", "c") 
})
```

``` js
// Load transit nodes in the selected region
loadTransitBtn = Grove.Button({
  label: "Load Transit",
  className: "btn-primary btn-lg customBtn",
  onClick: () => loadNodes("(t:Transit)", "t") 
})
```

``` js
// Remove the region selection. i.e. Make it so no region is selected
clearRegionBtn = Grove.Button({
  label: "Clear Region Selection",
  className: "btn-primary btn-lg customBtn",
  onClick: () => data.clearSelection()
})
```

``` js
// Expand the graph to crime nodes
// If there are selected junction nodes then they are expanded
// If there are no selected junction nodes then the entire graph is expanded
expandCrimeBtn = Grove.Button({
  label: "Expand To Crime Nodes",
  className: "btn-primary btn-lg customBtn",
  onClick: () => {
    let graph = api.getLayoutGraph();
    
    // Get the selected junction nodes
  	let nodes = graph.getVisibleNodes().filter(api.nodesByCategory("Junction")).filter(filterBySelection());
    
    // If there are no selected nodes then use all the nodes
    if (nodes.length == 0) nodes = graph.getVisibleNodes().filter(api.nodesByCategory("Junction"));
    let ids = nodes.map((node) => (node.properties.id));
    
    let query = `
    	MATCH (c:Crime)-[n:NEAREST_CRIME_JN]->(j:Junction)
        WHERE j.id IN [${ids}]
        RETURN *
    `
    
    api.neo4j(query);
  }
})
```

``` js
// Expand the network to include transit nodes
// If there are selected segment nodes then those are expanded
// otherwise all segment nodes are expanded
expandTransitBtn = Grove.Button({
  label: "Expand To Transit Nodes",
  className: "btn-primary btn-lg customBtn",
  onClick: () => {
    let graph = api.getLayoutGraph();
    
    // Get selected segment nodes
  	let nodes = graph.getVisibleNodes().filter(api.nodesByCategory("Segment")).filter(filterBySelection());
    
    // If there are no selected segment nodes then use all segment nodes
    if (nodes.length == 0) nodes = graph.getVisibleNodes().filter(api.nodesByCategory("Segment"));
    
    let ids = nodes.map((node) => (node.properties.id));
    let query = `
    	MATCH (t:Transit)-[n:PRESENT_IN]->(s:Segment)
        WHERE s.id IN [${ids}]
        RETURN *
    `
    
    api.neo4j(query);
  }
})
```

``` js
// Expand the road network
//   If there are selected junction or segment nodes then those are expanded
//   otherwise the whole road network is expanded
expandNetworkBtn = Grove.Button({
  label: "Expand Network",
  className: "btn-primary btn-lg customBtn",
  onClick: () => {
  	let graph = api.getLayoutGraph();
    
    // Get the segments and junctions and the selected segments and junctions
    let segments = graph.getVisibleNodes().filter(api.nodesByCategory("Segment"));
    let junctions = graph.getVisibleNodes().filter(api.nodesByCategory("Junction"));
    let selectedSegments = segments.filter(filterBySelection());
    let selectedJunctions = junctions.filter(filterBySelection());
    
    // Determine which nodes to use based on whether there are any selected nodes
    let useAll = selectedSegments.length == 0 && selectedJunctions.length == 0;
    let workingSegments = useAll ? segments : selectedSegments;
    let workingJunctions = useAll ? junctions : selectedJunctions;
    
    let segmentIds = workingSegments.map((segment) => (segment.properties.id));
    let junctionIds = workingJunctions.map((junction) => (junction.properties.id));
    
    let query = `
    	MATCH (s:Segment)-[c:CONTINUES_TO]->(j:Junction)
        WHERE (s.id IN [${segmentIds}]) OR 
        	  (j.id IN [${junctionIds}])
        RETURN *
    `
    
    api.neo4j(query);
  }
})
```

``` js
// Delete nodes that are selected
deleteSelectionBtn = Grove.Button({
  label: "Delete Selected Nodes",
  className: "btn-primary btn-lg customBtn",
  onClick: () => {
    // Delete selected nodes
    api.getLayoutGraph().applyTransform((graph) => graph.removeNodes(
    	graph.getVisibleNodes().filter(filterBySelection()).map((node) => node.id)
    ));
    
    // Update the display
    data.updateSelectedDisplay();
  }
})
```

``` js
// Select junctions in the region
selectJunctionsBtn = Grove.Button({
  label: "Select Junctions",
  className: "btn-primary btn-lg customBtn",
  onClick: () => selectCategory("Junction")
})
```

``` js
// Select any segment nodes in the region
selectSegmentsBtn = Grove.Button({
  label: "Select Segments",
  className: "btn-primary btn-lg customBtn",
  onClick: () => selectCategory("Segment")
})
```

``` js
// Select any crime nodes in the region
selectCrimesBtn = Grove.Button({
  label: "Select Crimes",
  className: "btn-primary btn-lg customBtn",
  onClick: () => selectCategory("Crime")
})
```

``` js
// Select any transit nodes in the region
selectTransitBtn = Grove.Button({
  label: "Select Transit",
  className: "btn-primary btn-lg customBtn",
  onClick: () => selectCategory("Transit")
})
```

``` js
// Unselect selected nodes
clearSelectionBtn = Grove.Button({
  label: "Unselect All",
  className: "btn-primary btn-lg customBtn",
  onClick: () => {
    api.getLayoutGraph().applyTransform((graph) => {
      let selectedNodes = graph.getVisibleNodes().filter(filterBySelection());
      selectedNodes.forEach((node) => node.setStyle("selected", false));
    });
   	data.updateSelectedDisplay();
  }
})
```

``` js
// Toggle data.showSelected which controls whether selected nodes are displayed on the map
toggleShowSelectedBtn = data.toggleShowSelectedBtn = Grove.Button({
  label: "Showing Selected",
  className: "btn-primary btn-lg customBtn",
  onClick: () => {
    // Toggle data.showSelected
    data.showSelected = !data.showSelected;
    
    // Update the button text
    let btn = data.toggleShowSelectedBtn;
  	btn.innerHTML = data.showSelected ? "Hide Selected" : "Show Selected";
    
    // Update the look of the button
    if (data.showSelected) {
      btn.classList.remove("btn-secondary");
      btn.classList.add("btn-primary");
    } else {
      btn.classList.add("btn-secondary");
      btn.classList.remove("btn-primary");
    }
    data.updateSelectedDisplay();
  }
})
```

``` js
// Select nodes that are part of [category]
//
// If there is a region selected then only select nodes in that region.
// otherwise select all nodes in [category]
//
// Parameters
// category (string): The category to select nodes from
selectCategory = function(category) {
    api.getLayoutGraph().applyTransform((graph) => {
      // Select nodes that are in the region and have the category, [category]
      let nodesToSelect = graph.getVisibleNodes()
                               .filter(api.nodesByCategory(category))
                               .filter(filterByRegion());
      // Mark the nodes as selected
      nodesToSelect.forEach((node) => node.setStyle("selected", true));
    });
   	data.updateSelectedDisplay();
}
```

``` js
// Generate a clause that can be used as part of the cypher WHERE clause to only select
// nodes in the selected region
//
// Parameters
// nodeName (string): The name used to refer to the nodes in the cypher query. Eg: MATCH (j:Junction) would have j as the node name
//
// Returns
// string: The clause that can be used as part of the WHERE
function generateLocationWhere(nodeName) {
  let lat1 = 0, lat2 = 0, lng1 = 0, lng2 = 0;
  [lat1, lat2] = [data.positions.lat1, data.positions.lat2].sort((a, b) => a-b);
  [lng1, lng2] = [data.positions.lng1, data.positions.lng2].sort((a, b) => a-b);
  
  return `
  	${nodeName}.latitude >= ${lat1} AND ${nodeName}.latitude <= ${lat2} AND
    ${nodeName}.longitude >= ${lng1} AND ${nodeName}.longitude <= ${lng2}
  `
}
```

``` js
// Determine whether or not a region is currently selected
//
// Returns
// boolean: true if a region is selected, false otherwise
isRegionSelected = () => data.positions.lat1 != data.positions.lat2 || data.positions.lng1 != data.positions.lng2
```

``` js
// Load nodes from Neo4j using the given [match] clause
//
// If a region is selected then only nodes from that region will be selected,
// otherwise all nodes that [match] finds will be loaded
//
// Parameters
// match (string): The clause to use for matching nodes in the Cypher query. eg: '(j:Junction)'
// longitude_limiter (string): The name of the node that will be used to limit to the region. eg: For the above, 'j'
function loadNodes(match, longitude_limiter) {
  let where = isRegionSelected() ? `WHERE ${generateLocationWhere(longitude_limiter)}` : ''
    
  let query = `
      MATCH ${match}
      ${where}
      RETURN * 
    `;

  api.neo4j(query);
}
```

``` js
// Filter for nodes that are in the region
//
// If no region is selected then no filtering is done
//
// Returns
// function (n)->boolean: The function that can be used as a filter on an array of nodes
function filterByRegion() {
  // Get the region positions in order
  let lat1 = 0, lat2 = 0, lng1 = 0, lng2 = 0;
  [lat1, lat2] = [data.positions.lat1, data.positions.lat2].sort((a, b) => a-b);
  [lng1, lng2] = [data.positions.lng1, data.positions.lng2].sort((a, b) => a-b);
  
  let hasRegion = isRegionSelected();
  
  return (node) => {
    if (!hasRegion) return true;
    let latidude = node.properties.latitude;
    let longitude = node.properties.longitude;
    return latidude >= lat1 && latidude <= lat2 &&
      longitude >= lng1 && longitude <= lng2;
  }
}
```

``` js
// Filter for nodes that are selected
//
// Returns
// function (n)->boolean: The function that can be used as a filter on an array of nodes
function filterBySelection() { return (n => n.getStyle('selected')) }
```

``` js
// A handy button for development that makes the Grove and GraphXR APIs availiable in the inspector
Grove.Button({
  label: "Make APIs Visible",
  onClick: () => {
    window.Grove = Grove;
    window.api = api;
  }
})
```
