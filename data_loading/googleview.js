//Testing the javascript codes for the Grove file - where running of Street View content is written.


//Invoking the GraphXR api 
api=(await require('@kineviz/graphxr-api@0.0.227')).getApi()

//Fetching the properties of the node selected
selectedNodes = api.observe('select',  () => {
  let view = api.getLayoutGraph()
  return view.getNodes()
    .filter(n=>view.getNodeStyles(n.id).selected)
});

//Assigning the fetched properties to selectedNodeProps
selectedNodesProps = selectedNodes.map(n=>n.properties)

//Instantiating the google object - Google Maps API retrieval
google = await require('https://maps.googleapis.com/maps/api/js?key=AIzaSyBUZHBvEZffec9_A9ovpaUD8nyOIpcXhTA').catch(() => window.google)

//Using the latitude and longitude from the selectedNode properties to dynamically render the Google Maps and Street View content
Chart = {
  let div = html`<div style='height:400px;'></div>`

  const mapHolder = [0]
  
  yield div;
  let map = new google.maps.Map(div, {
    center: { lng: selectedNodesProps[0]["longitude"], lat: selectedNodesProps[0]["latitude"]},
    zoom: 18
  });
  mapHolder[0] = map;
 let point=[{x:selectedNodesProps[0]["longitude"], y:selectedNodesProps[0]["latitude"]}]
  point.map(
    p =>
      new google.maps.Marker({
        position: { lng: p.x, lat: p.y },
        map: mapHolder[0]
      })
  );
}

  //Inputs.table(api.getLayoutGraph().getNodes().map(n=>n.properties))

  // api.observe('change',()=>{
  //   mutable testNodes=api.getLayoutGraph().getNodes()
  // //     .map(n=>n.properties)
  // })

  //selectedNodesProps = selectedNodes.map(n=>n.properties)

 

  



