# <center> Monitoring Tropical Storms Project </center>
---
Monitoring tropical storms through publicly available datasets. The information for this project came from the [Monitoring Tropical Storms for Emergency Preparedness](https://appliedsciences.nasa.gov/get-involved/training/english/arset-monitoring-tropical-storms-emergency-preparedness) training, which is part of the [NASA Applied Remote Sensing Training Program (ARSET)](https://arset.gsfc.nasa.gov/).

---

### Data Formats

| Source Format | Type | Description | Examples |
| --- | --- | --- | --- |
| Shapefile | Vector & Raster |	A vector data format that was developed by Esri. It lets you store geometric locations and associate attributes. | Census tract geometries, building footprints |
| [WKT](https://wikipedia.org/wiki/Shapefile) |	Vector | A human-readable vector data format that's published by OGC. Support for this format is built into BigQuery. | Representation of geometries in CSV files |
| [WKB](https://wikipedia.org/wiki/Well-known_text_representation_of_geometry#Well-known_binary) | Vector |	A storage-efficient binary equivalent of WKT. Support for this format is built into BigQuery. | Representation of geometries in CSV files and databases |
| KML | ??? |	An XML-compatible vector format used by Google Earth and other desktop tools. The format is published by OGC. | 3D building shapes, roads, land features |
| Geojson | ??? | An open vector data format that's based on JSON. | Features in web browsers and mobile applications |
| GeoTIFF | ??? | A widely used raster data format. This format lets you map pixels in a TIFF image to geographic coordinates. | Digital elevation models, Landsat
| NetCDF | ??? | A widely used raster data format that's used for scientific data. This format lets you store multidimensional data in a single file. | Climate data, ocean data |

### Data Sources


---
---
---