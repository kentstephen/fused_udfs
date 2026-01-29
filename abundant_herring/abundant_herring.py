@fused.udf
def udf(bounds: fused.types.Bounds=None):
    import fsspec
    import xml.etree.ElementTree as ET
    
    bucket_path = "gs://fused-fd-stephenkentdata/stephenkentdata/DEM_from_USGS/grand_canyon/"
    
    fs = fsspec.filesystem('gcs')
    
    # List all files in the directory
    all_files = fs.glob(f"{bucket_path}*")
    
    # Find XML files
    xml_files = [f for f in all_files if f.endswith('.xml')]
    
    if not xml_files:
        print("No XML files found")
        return None
    
    print(f"Found {len(xml_files)} XML files:")
    for f in xml_files:
        print(f"  - {f.split('/')[-1]}")
    
    # Read the first XML file
    first_xml = f"gs://{xml_files[0]}"
    print(f"\nReading: {first_xml}")
    print("=" * 80)
    
    with fs.open(xml_files[0], 'r') as f:
        xml_content = f.read()
        
    # Parse XML
    root = ET.fromstring(xml_content)
    
    # Print the entire XML structure with indentation
    def print_element(element, indent=0):
        spaces = "  " * indent
        if element.text and element.text.strip():
            print(f"{spaces}{element.tag}: {element.text.strip()}")
        else:
            print(f"{spaces}{element.tag}:")
        for child in element:
            print_element(child, indent + 1)
    
    print_element(root)
    
    # return xml_content