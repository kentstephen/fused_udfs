# Fused.io UDFs Collection                                                    
                                                                            
700+ User Defined Functions for [Fused.io](https://fused.io) - a platform for 
serverless geospatial compute and analysis.

These are UDFs I have worked on since April of 2024.

Includes:                                                             
- Overture Maps building footprints                                           
- Sentinel satellite imagery processing                                       
- H3 hexagon aggregations                                                     
- DEM terrain analysis                                                        
                                                                            

## Note

- Any tokens or API keys referenced in these UDFs won't work
- Data stored in my personal Fused object storage won't be accessible
- Be careful with the Zarr UDFs - they can be compute intensive

## Folders

- [udfs](./udfs) - Unzipped UDFs for browsing
- [udfs_just_py](./udfs_just_py) - Just the Python files 
- [udfs_zipped](./udfs_zipped) - Zip files for uploading directly into Fused

 Older UDFs will refer to a `utils.py` that was deprecated in order to keep all the code in the main Python file. You would need to reactivate utils in the setttings to gain access to that file in Fused. If the UDFs have `utils.py` included they would available in [udfs](./udfs) and [udfs_zipped](./udfs_zipped).
