__version__ = ""

from meshroom.core import desc

import os
import csv
import json
import struct
import itertools

class ImportMarkerFeatures(desc.Node):
    category = 'Utils'
    size = desc.StaticNodeSize(1)
    parallelization = None
    gpu = desc.Level.NONE
    cpu = desc.Level.NORMAL
    ram = desc.Level.NORMAL
    documentation = '''
This node is a utility to help in the use of marker matches produced by third-party software.
Unfortunately the native Meshroom support for marker detection is lacking important parameters, and hard to predict.
Currently there is no way to manually register markers on images to enhance the reconstruction and georeferencing.

This node reads a formatted CSV file containing the data of markers, and produces cctag3 or cctag4 feature descriptors.
CCTag3 and 4 markers are represented by their single center point, and they are supported in both the Windows and Linux versions of Meshroom.
Practically any marker can be passed off as a CCTag marker, provided that its center point can be determined.
Each line of the CSV must define 1 marker on 1 image.

CSV format:

    markerX, markerY, imageFileName, markerID, markerSize
    
    markerX:            horizontal image coordinate of marker in pixels
    markerY:            vertical image coordinate of marker in pixels
    imageFileName:      name of image file including extension (case sensitive)
    markerID:           unique ID of marker
    markerSize:         size of marker in pixels
'''
    
    inputs = [
        desc.File(
            name = "input",
            label = "SfMData",
            description = "Input SfMData file.",
            value = "",
            uid = [0]
        ),
        desc.File(
            name = "matches",
            label = "Marker Features Data",
            description = "CSV file containing image coordinates of markers, marker ID and marker size (in pixels).",
            value = "",
            uid = [0]
        ),
        desc.BoolParam(
            name = "hack",
            label = "Enable to Bypass 128-Tag Limit",
            description = "This option skips the FeatureMatching node and directly generates a matches.txt file for cctag match.",
            value = False,
            uid = [0]
        ),
        desc.ChoiceParam(
            name = "delimiter",
            label = "Delimiter",
            description = "Delimiter character used in the input CSV file.",
            value = "comma",
            values = ["space", "tab", "comma", "colon", "semicolon"],
            exclusive = True,
            uid = [0]
        ),
        desc.ChoiceParam(
            name = "type",
            label = "Import As",
            description = "Descriptor type to create for the imported marker data.",
            value = "cctag3",
            values = ["cctag3", "cctag4"],
            exclusive = True,
            uid = [0]
        ),
        desc.ChoiceParam(
            name = "verboseLevel",
            label = "Verbose Level",
            description = "Verbosity level (fatal, error, warning, info, debug, trace).",
            value = "info",
            values = ["fatal", "error", "warning", "info", "debug", "trace"],
            exclusive = True,
            uid = []
        )
    ]
    
    outputs = [
        desc.File(
            name = "output",
            label = "Marker Features Folder",
            description = "Output path for the features and descriptors files (*.feat, *.desc).",
            value = desc.Node.internalFolder,
            uid = []
        ),
        desc.File(
            name = "matches_out",
            label = "Matches Folder",
            description = "Link to the SFM node's Matches Folder input, witch supports multiple elements.",
            value = desc.Node.internalFolder,
            uid = []
        )
    ]
    
    def load_images(self, chunk, filepath, delimiter):
        images = {}
        csv_data = []
        
        with open(filepath) as file:
            gcp_file = csv.reader(file, delimiter=delimiter)
            csv_data = [(row[2], row[0], row[1], row[4], int(row[3])) for row in gcp_file]

        images.update({item[0]: [] for item in csv_data})

        for item in csv_data:
            images[item[0]].append([*item[1:]])
        
        chunk.logger.info("Loaded %d marker matches in %d image(s)" % (len(csv_data), len(images)))
            
        return images
    
    def load_viewids(self, chunk):
        if not os.path.isfile(chunk.node.input.value):
            raise Exception("View data file not found")
        
        views = []
        views_lookup = {}
        with open(chunk.node.input.value) as file:
            views = json.load(file)["views"]
        
        for item in views:
            views_lookup[os.path.basename(item["path"])] = item["viewId"]
        
        chunk.logger.info("Found %d view(s)" % len(views_lookup))
        
        return views_lookup
    
    def write_describers(self, chunk, images, lookup):
        chunk.logger.info("Writing %s descriptor files" % chunk.node.type.value)
        chunk.logManager.makeProgressBar(len(lookup))
        
        found_markers = {i: 0 for i in list(set([marker[3] for img in images for marker in images[img]]))}
        feature_lookup = {viewid: {} for viewid in lookup.values()} # feature_lookup[viewid][tagid]
        
        for i, img in enumerate(lookup):
            viewid = lookup[img]
            
            feat = open(os.path.join(chunk.node.output.value, viewid + (".%s.feat" % chunk.node.type.value)), "w")
            desc = open(os.path.join(chunk.node.output.value, viewid + (".%s.desc" % chunk.node.type.value)), "wb")
            
            if img in images:
                feat_idx = 0
                markers = images[img]
                desc.write(struct.pack('<Q', len(markers)))
                
                for marker in markers:
                    feat_x, feat_y, feat_size, feat_orientation = marker[0], marker[1], marker[2], "0"
                    tagid = marker[3]
                    found_markers[tagid] += 1
                    feat.write(" ".join((feat_x, feat_y, feat_size, feat_orientation + "\n")))
                    feature_lookup[viewid][tagid] = str(feat_idx)
                    feat_idx += 1
                    
                if chunk.node.hack.value is False:
                    data = bytearray(128)
                    data[marker[3]] = 255
                    desc.write(data)
                
            else:
                desc.write(struct.pack('<Q', 0))
            
            desc.close()
            feat.close()
            
            chunk.logManager.updateProgressBar(i + 1)
            
        chunk.logger.info("Markers report:")
        for marker in found_markers:
            chunk.logger.info("\tFound marker %d in %d view(s)" % (marker, found_markers[marker]))
            
        return feature_lookup
    
    def make_matches_txt(self, chunk, image_pairs, feature_lookup):
        out_temp = []
        for pair in image_pairs:
            viewid_A, viewid_B = pair
            tags_detected_A = feature_lookup[viewid_A].keys()
            tags_detected_B = feature_lookup[viewid_B].keys()
            match = tags_detected_A & tags_detected_B
            if match:
                out_temp.append(" ".join((viewid_A, viewid_B)))
                out_temp.append("1")
                out_temp.append(f"{chunk.node.type.value} {len(match)}")
                for tagid in match:
                    feature_index_A = feature_lookup[viewid_A][tagid]
                    feature_index_B = feature_lookup[viewid_B][tagid]
                    out_temp.append(" ".join((feature_index_A, feature_index_B)))
        
        with open(os.path.join(chunk.node.matches_out.value, "0.matches.txt"), "w") as matches_txt:
            matches_txt.write("\n".join(out_temp))
    
    def processChunk(self, chunk):
        delimiters_options = {
            "space": " ",
            "tab": "\t",
            "comma": ",",
            "colon": ":",
            "semicolon": ";"
        }
        
        try:
            chunk.logManager.start(chunk.node.verboseLevel.value)
            
            chunk.logger.info("Importing marker data")
            
            if not os.path.isfile(chunk.node.matches.value):
                raise OSError("Marker features list file not found")
            
            lookup = self.load_viewids(chunk)
            image_pairs = list(itertools.combinations(lookup.values(), 2)) # This method can produce the same results as the ImageMatching node that uses the "Exhaustive" method.
            images = self.load_images(chunk, chunk.node.matches.value, delimiters_options[chunk.node.delimiter.value])
            feature_lookup = self.write_describers(chunk, images, lookup)
            if chunk.node.hack.value:
                self.make_matches_txt(chunk, image_pairs, feature_lookup)
            
            chunk.logger.info("Task done")
            
            
        except Exception as e:
            chunk.logger.error(e)
            raise
        finally:
            chunk.logManager.end()
