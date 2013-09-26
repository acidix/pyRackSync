#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Python Module to push (one way) to Racktables

import logging

class RacktablesSyncClient(object):

    def __init__(self, RackTablesClient):
        """Initialize the class

        :param RackTablesClient: RackTablesClient object (ibettinger's Python module)

        """
        super(RacktablesSyncClient, self).__init__()
        self.rtClient = RackTablesClient

    def doSyncObject (self, syncObject):
        """Function to be called to synchronize an object

        Data Structure:

              syncObject{ "name": string,
                          "objtype_id": string,
                          "network": { "iface": { "name": string,
                                                  "mac": string,
                                                  "fqdn": string,
                                                  "ip": string,
                                                  "force": boolean
                                                 }
                                      }
                          }
                          "attrs": [ { "attrid" : "attrval" } ],
                          "forceattrs": ["attrid"]
              }

        :param syncObject: Dictionary object with special structure.
        """
        objtype_id = syncObject["objtype_id"]

        objects = self.rtClient.get_objects(None, False, objtype_id)
        objects = filter(lambda obj: obj["name"].lower() == syncObject["name"].lower(), objects.values())

        ## Object does not exist
        if len(objects) == 0:
            self.doAddObject(syncObject)

        ## Objects exists
        if len(objects) == 1:
            targetObj = self.doBuildObjectTree(objects[0]["id"])
            self.doSync(targetObj, syncObject)

        ## Multiple objects exist
        if len(objects) > 1:
            logging.exception("More than one matching objects found.")



    def doBuildObjectTree(self, obj_id):
        """Build an object tree from RackTables

        :param obj_id: Object ID for which to build the tree
        :returns: Dictionary with information pulled from RackTables

        """
        logging.debug("Object found in Racktables. Building sync tree")

        rtObjectRaw = self.rtClient.get_object(obj_id, True, True)
        rtObject = {}
        initProperties = ["name", "id", "asset_no", "label", "objtype_id", "label", "comment"]

        for prop in initProperties:
            rtObject[prop]    = rtObjectRaw[prop]

        rtObject["attrs"]   = {}
        rtObject["network"] = {}

        for idx, rtObjectRawNet in rtObjectRaw["ports"].items():
            logging.debug("Processing port " + rtObjectRawNet["name"])

            rtObject["network"][rtObjectRawNet["name"]] = {
                "name": rtObjectRawNet["name"],
                "mac" : rtObjectRawNet["l2address"],
                "fqdn": rtObjectRawNet["label"],
                "id"  : rtObjectRawNet["id"]
            }

            for idx, rtObjectRawIP in rtObjectRaw["ipv4"].items():
                if len(rtObjectRawIP["addrinfo"]["allocs"]) != 1:
                    raise Exception("Multiple allocations for IP:" + rtObjectRawIP["addrinfo"]["ip"])

                if rtObjectRawIP["osif"] == rtObjectRawNet["name"]:
                    rtObject["network"][rtObjectRawNet["name"]]["ip"] = rtObjectRawIP["addrinfo"]["ip"]

        for idx, rtObjectRawAttr in rtObjectRaw["attrs"].items():
            rtObject["attrs"][rtObjectRawAttr["id"]] = rtObjectRawAttr["value"]

        return rtObject




    def doAddObject(self, syncObject):
        """Add an object to RackTables

        :param syncObject: The dictionary object to create

        """
        logging.debug("Object NOT found in Racktables. Creating it.")

        if not "attrs" in syncObject:
            syncObject["attrs"] = {}

        newObject = self.rtClient.add_object(syncObject["name"],
            None,
            syncObject["name"],
            syncObject["objtype_id"],
            None,
            [],
            syncObject["attrs"])

        ## Delete all implicitly created ports
        try:
            for idx, newObjectPort in newObject["ports"].items():
                self.doDeletePort(newObject["id"], newObjectPort["id"])
        except Exception, errtxt:
            logging.exception("Error dropping ports: " + str(errtxt))

        ## Add ports
        logging.debug("Adding network ports")
        try:
            for idx, syncObjectNet in syncObject["network"].items():
                self.doAddPort(newObject["id"], syncObjectNet)

                if "ip" in syncObjectNet:
                    self.doAddIP(newObject["id"],
                        syncObjectNet["ip"],
                        syncObjectNet["name"])
        except Exception, errtxt:
            logging.exception("Error adding network port: " + str(errtxt))

        if syncObject["linkparent"]:
            try:
                self.rtClient.link_entities(newObject["id"], syncObject["linkparent"])
            except Exception, errtxt:
                logging.exception("Error linking entities: " + str(errtxt))




    def doSync(self, rtObject, syncObject):
        """Synchronize object to RackTables

        :param rtObject: The dictionary RackTables object
        :param syncObject: The dictionary object to create

        """
        logging.debug("The object does exist, sync it")

        globalforce = syncObject["network"].get("force", False)
        try:
            del syncObject["network"]["force"]
        except KeyError:
            pass

        for rtObjectIdx, rtObjectNet in rtObject["network"].items():

            syncObjectNetFiltered = filter(lambda (idx, net): net["name"].lower() == rtObjectNet["name"].lower() and
                net["mac"].lower() == rtObjectNet["mac"].lower(), syncObject["network"].items())

            if not syncObjectNetFiltered and globalforce:
                self.doDeletePort(rtObjectNet)
                continue

            if len(syncObjectNetFiltered) > 1:
                raise Exception("Multiple matching ports in sync object. Aborting.")

            syncObjectNet = syncObjectNetFiltered[0][1]
            force = syncObjectNet.get("force", False)

            if syncObjectNet["fqdn"].lower() != rtObjectNet["fqdn"].lower() and force:
                self.doDeletePort(rtObject["id"], rtObjectNet)
                self.doAddPort(rtObject["id"], rtObjectNet)

            syncIP = syncObjectNet.get("ip", rtObjectNet.get("ip", False))
            rtIP = rtObjectNet.get("ip", False)

            if syncIP:
                if not rtIP:
                    self.doAddIP(rtObject["id"], syncIP, syncObjectNet["name"])
                    rtIP = syncIP
                if syncIP != rtIP and force:
                    self.doDeleteIP(rtObject["id"], rtIP)
                    self.doAddIP(rtObject["id"], syncIP, syncObjectNet["name"])

            del syncObject["network"][syncObjectNetFiltered[0][0]]


        ## Now add the ports we're left with
        for syncObjectNetIdx, syncObjectNet in syncObject["network"].items():
            self.doAddPort(rtObject["id"], syncObjectNet)
            if "ip" in syncObjectNet:
                self.doAddIP(rtObject["id"], syncObjectNet["ip"], syncObjectNet["name"])

        if not "attrs" in syncObject:
            return

        ## Synchronize attrs
        for o_attrname, o_attrval in rtObject["attrs"].items():
            if o_attrname in syncObject["attrs"] and o_attrname in syncObject["forceattrs"]:
                continue

            syncObject["attrs"][o_attrname] = o_attrval


        logging.debug("Updating attributes")
        self.rtClient.edit_object(rtObject["id"], rtObject["name"], rtObject["asset_no"], rtObject["label"], rtObject["objtype_id"], rtObject["comment"], syncObject["attrs"])

    def doAddPort(self,objectID,port):
        """Add Port in RackTables

        :param objectID: ID of the object to add the port to (String)
        :param port: Port dictionary (Dict)
        :returns: ID of new port

        """
        return self.rtClient.add_object_port(objectID, port["name"], port["mac"], "1-24", port["fqdn"])

    def doDeletePort(self, objectID, port):
        """Delete a port and its IP address

        :param objectID: ID of the object to add the port to (String)
        :param port: Port dictionary (Dict)

        """
        if "ip" in port:
            self.doDeleteIP(objectID, port["ip"])
        self.rtClient.delete_object_port(objectID, port["id"])

    def doAddIP(self, objectID, ip, iface):
        """Add IP to object

        :param objectID: ID of the object to add the port to (String)
        :param ip: IP Address (String)
        :param iface: Interface (String)

        """
        self.rtClient.add_object_ipv4_address(objectID, ip, iface)

    def doDeleteIP(self,objectID,ip):
        """Delete IP from object

        :param objectID: ID of the object to add the port to (String)
        :param ip: IP Address (String)

        """
        self.rtClient.delete_object_ipv4_address(objectID, ip)
