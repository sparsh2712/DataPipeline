import xml.etree.ElementTree as ET

def extract_xbrl_governance(file_path, output_file):
    tree = ET.parse(file_path)
    root = tree.getroot()
    
    namespaces = {
        "xbrli": "http://www.xbrl.org/2003/instance",
        "in-bse-cg": "http://www.bseindia.com/xbrl/cg/2024-03-31/in-bse-cg",
    }
    
    data = {
        "board_members": [],
        "committee_members": [],
        "board_meetings": [],
        "committee_meetings": [],
        "related_party_transactions": [],
        "affirmations": [],
        "cyber_security_incidents": []
    }
    
    for element in root.findall(".//xbrli:context", namespaces):
        scenario = element.find(".//xbrli:scenario", namespaces)
        if scenario is not None:
            for key, tag in {
                "board_members": "CompositionOfBoardOfDirectorsDomain",
                "committee_members": "CompositionOfCommitteesDomain",
                "board_meetings": "MeetingOfBoardOfDirectorsDomain",
                "committee_meetings": "MeetingOfCommitteesDomain",
                "related_party_transactions": "RelatedPartyTransactionsDomain",
                "affirmations": "AffirmationsDomain",
                "cyber_security_incidents": "CyberSecurityIncidentsDomain"
            }.items():
                element_data = scenario.find(f".//in-bse-cg:{tag}", namespaces)
                if element_data is not None:
                    data[key].append(element_data.text)
    
    # Extract director names
    data["director_names"] = []
    for director in root.findall(".//in-bse-cg:NameOfDirector", namespaces):
        if director.text:
            data["director_names"].append(director.text)
    
    with open(output_file, "w") as f:
        for key, values in data.items():
            f.write(f"{key.replace('_', ' ').title()}:\n")
            if values:
                for value in values:
                    f.write(f"  - {value}\n")
            else:
                f.write("  No data available\n")
            f.write("\n")
    
    print(f"Extracted data has been saved to {output_file}")

# Example Usage
extract_xbrl_governance("input.xbrl", "output.txt")