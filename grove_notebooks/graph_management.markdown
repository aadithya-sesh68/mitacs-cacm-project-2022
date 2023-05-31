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
              children: []	
            })
          ]
        })
    ]
})
```

``` js
{
  const vancouver_pos = [49.25, -123];
  const container = yield htl.html`<div style="height: 600px;">`;
  const map = L.map(container).setView(vancouver_pos, 12);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "© <a href=https://www.openstreetmap.org/copyright>OpenStreetMap</a> contributors"
  }).addTo(map);
  
  let positions = {lat1: 0, lng1: 0, lat2: 0, lng2: 0};
  data.positions = positions;
  data.map_container = container;
  data.map = map;
  data.selectedNodesLayer = L.layerGroup().addTo(map);
  
  let marker1 = L.marker();
  let marker2 = L.marker();
  let polygon = L.polygon([]);
  
  function onMapClick(e) {
    let first = !e.originalEvent.shiftKey;
    if (first) {
      positions.lat1 = e.latlng.lat;
      positions.lng1 = e.latlng.lng;
      marker1.setLatLng(e.latlng).addTo(map);
      
      if(positions.lat2 == 0 && positions.lng2 == 0) {
       	positions.lat2 = positions.lat1;
        positions.lng2 = positions.lng1;
      }
    } else {
      positions.lat2 = e.latlng.lat;
      positions.lng2 = e.latlng.lng;
      marker2.setLatLng(e.latlng).addTo(map);
      
      if(positions.lat1 == 0 && positions.lng1 == 0) {
       	positions.lat1 = positions.lat2;
        positions.lng1 = positions.lng2;
      }
    }
    
    let latlngs = [
      [positions.lat1, positions.lng1], 
      [positions.lat1, positions.lng2],
      [positions.lat2, positions.lng2],
      [positions.lat2, positions.lng1]
    ];
    polygon.setLatLngs(latlngs).addTo(map);
  }
  
  function clearSelection() {
    positions.lat1 = 0;
    positions.lat2 = 0;
    positions.lng1 = 0;
    positions.lng2 = 0;
    
    marker1.remove();
    marker2.remove();
    polygon.remove();
  }
  
  data.clearSelection = clearSelection;
  
  let icons = {
    default: L.divIcon({className: 'selectedNode'}),
    Junction: L.divIcon({className: 'selectedNode selectedJunction'}),
    Segment: L.divIcon({className: 'selectedNode selectedSegment'}),
    Crime: L.divIcon({className: 'selectedNode selectedCrime'}),
    Transit: L.divIcon({className: 'selectedNode selectedTransit'})
  }
  
  async function updateSelectedDisplay() {
    let graph = api.getLayoutGraph();
    let selectedNodes = graph.getVisibleNodes().filter(n=>graph.getNodeStyles(n.id).selected);
    data.selectedNodesLayer.clearLayers();
    
    selectedNodes.forEach((node) => {
      let lat = node.properties.latitude;
      let lng = node.properties.longitude;
      
      let icon = icons.default;
      if (node.category in icons)
        icon = icons[node.category]
      
      data.selectedNodesLayer.addLayer(L.marker([lat, lng], {icon: icon}));
    })
  }
  data.updateSelectedDisplay = updateSelectedDisplay;
  api.observe("select", updateSelectedDisplay);
  
  map.on('click', onMapClick);
}
```

``` js
<style>
  .customBtn { 
    width: 200px;
    height: 70px;
  }
  
  .selectedNode {
    background-color: black;
    border-style: solid;
    border-color: black;
    border-width: 2px;
    padding: 5px;
    border-radius: 100px;
  }
  
  .selectedJunction {
    background-color: orange;
  }
  
  .selectedSegment {
   	 background-color: grey;
  }
  
  .selectedCrime {
   	 background-color: red;
  }
  
  .selectedTransit {
   	 background-color: blue;
  }
</style>
```

``` js
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
toggleMapBtn = Grove.Button({
  label: "Toggle Map",
  className: "btn-primary btn-lg customBtn",
  onClick: () => {
    if (data.map_container.style.display == "none") {
      data.map_container.style.display = "";
    } else {
      data.map_container.style.display = "none"; 
    }
  }
})
```

``` js
removeSelectedRegionBtn = Grove.Button({
  label: "Remove Selected Region",
  className: "btn-primary btn-lg customBtn",
  onClick: () => {
  	let lat1 = 0, lat2 = 0, lng1 = 0, lng2 = 0;
    [lat1, lat2] = [data.positions.lat1, data.positions.lat2].sort((a, b) => a-b);
    [lng1, lng2] = [data.positions.lng1, data.positions.lng2].sort((a, b) => a-b);
    
    let graph = api.getLayoutGraph();
    graph.applyTransform((graph) => {
      graph.removeNodes(
      	graph.getNodes().filter((node) => {
          let latidude = node.properties.latitude;
          let longitude = node.properties.longitude;
          return latidude >= lat1 && latidude <= lat2 &&
            	 longitude >= lng1 && longitude <= lng2;
        }).map((node) => node.id)
      );
    });
  }
})
```

``` js
loadSelectedRegionBtn = Grove.AsyncButton({
  label: "Load Selected Region",
  className: "btn-primary btn-lg customBtn",
  onClick: () => loadNodes("(s:Segment)-[c:CONTINUES_TO]->(j:Junction)", "j")
})
```

``` js
loadJunctionsBtn = Grove.Button({
  label: "Load Junctions",
  className: "btn-primary btn-lg customBtn",
  onClick: () => loadNodes("(j:Junction)", "j") 
})
```

``` js
loadSegmentsBtn = Grove.Button({
  label: "Load Segments",
  className: "btn-primary btn-lg customBtn",
  onClick: () => loadNodes("(s:Segment)", "s") 
})
```

``` js
clearRegionBtn = Grove.Button({
  label: "Clear Region Selection",
  className: "btn-primary btn-lg customBtn",
  onClick: () => data.clearSelection()
})
```

``` js
loadCrimesBtn = Grove.Button({
  label: "Load Crimes",
  className: "btn-primary btn-lg customBtn",
  onClick: () => loadNodes("(c:Crime)", "c") 
})
```

``` js
loadTransitBtn = Grove.Button({
  label: "Load Transit",
  className: "btn-primary btn-lg customBtn",
  onClick: () => loadNodes("(t:Transit)", "t") 
})
```

``` js
expandCrimeBtn = Grove.Button({
  label: "Expand To Crime Nodes",
  className: "btn-primary btn-lg customBtn",
  onClick: () => {
    let graph = api.getLayoutGraph();
  	let nodes = graph.getVisibleNodes().filter(api.nodesByCategory("Junction")).filter(n=>graph.getNodeStyles(n.id).selected);
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
expandTransitBtn = Grove.Button({
  label: "Expand To Transit Nodes",
  className: "btn-primary btn-lg customBtn",
  onClick: () => {
    let graph = api.getLayoutGraph();
  	let nodes = graph.getVisibleNodes().filter(api.nodesByCategory("Segment")).filter(n=>graph.getNodeStyles(n.id).selected);
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
expandNetworkBtn = Grove.Button({
  label: "Expand Network",
  className: "btn-primary btn-lg customBtn",
  onClick: () => {
  	let graph = api.getLayoutGraph();
    let segments = graph.getVisibleNodes().filter(api.nodesByCategory("Segment"));
    let junctions = graph.getVisibleNodes().filter(api.nodesByCategory("Junction"));
    let selectedSegments = segments.filter(n=>graph.getNodeStyles(n.id).selected);
    let selectedJunctions = junctions.filter(n=>graph.getNodeStyles(n.id).selected);
    
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
clearSelectionBtn = Grove.Button({
  label: "Unselect All",
  className: "btn-primary btn-lg customBtn",
  onClick: () => {
    let graph = api.getLayoutGraph();
    graph.applyTransform((graph) => {
    	let selectedNodes = graph.getVisibleNodes().filter(n=>graph.getNodeStyles(n.id).selected);
    	selectedNodes.forEach((node) => node.setStyle("selected", false));
    });
   	data.updateSelectedDisplay();
  }
})
```

``` js
selectCategory = function(category) {
  let graph = api.getLayoutGraph();
    graph.applyTransform((graph) => {
    	let nodesToSelect = graph.getVisibleNodes().filter(api.nodesByCategory(category));
    	nodesToSelect.forEach((node) => node.setStyle("selected", true));
    });
   	data.updateSelectedDisplay();
}
```

``` js
selectJunctionsBtn = Grove.Button({
  label: "Select Junctions",
  className: "btn-primary btn-lg customBtn",
  onClick: () => selectCategory("Junction")
})
```

``` js
selectSegmentsBtn = Grove.Button({
  label: "Select Segments",
  className: "btn-primary btn-lg customBtn",
  onClick: () => selectCategory("Segment")
})
```

``` js
selectCrimesBtn = Grove.Button({
  label: "Select Crimes",
  className: "btn-primary btn-lg customBtn",
  onClick: () => selectCategory("Crime")
})
```

``` js
selectTransitBtn = Grove.Button({
  label: "Select Transit",
  className: "btn-primary btn-lg customBtn",
  onClick: () => selectCategory("Transit")
})
```

``` js
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
isRegionSelected = () => data.positions.lat1 != data.positions.lat2 || data.positions.lng1 != data.positions.lng2
```

``` js
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
Grove.Button({
  label: "Make APIs Visible",
  onClick: () => {
    window.Grove = Grove;
    window.api = api;
  }
})
```
