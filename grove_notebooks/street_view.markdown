``` js
button=html`<button class="btn btn-primary">Open Street View</button>`
```

``` js
mapHolder=html`<div style='height:700px;'></div>`
```

``` js
API = (await require('@kineviz/graphxr-api@0.0.227')).getApi()
```

``` js
google = require('https://maps.googleapis.com/maps/api/js?key=' + Secret("Google API key")).catch(() => window.google)
```

``` js
button.onclick = function () {
  let view = API.getLayoutGraph()
  
  //Fetching the properties of the node selected
  let selectedNodes = view.getVisibleNodes().filter(n=>view.getNodeStyles(n.id).selected)
  
  let selectedNode = selectedNodes[0].properties
  let position = {lng: selectedNode["longitude"], lat: selectedNode["latitude"]}
  
  let panorama = new google.maps.StreetViewPanorama(
    mapHolder,
    {
      position: position,
      pov: {
        heading: 0,
        pitch: 0
      }
    }
  )
}
```
