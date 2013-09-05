#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Python Module to push (one way) to Racktables

import logging

class RacktablesSyncClient(object):

    def __init__(self, RackTablesClient):
        super(RacktablesSyncClient, self).__init__()
        self.rtClient = RackTablesClient

    def syncServer (self, objtosync, objtype_id='4', linkparent=False):
        try:
            objects = {}
            objects = self.rtClient.get_objects(None, False, objtype_id)
        except Exception, errtxt:
            logging.exception("Error initializing object tree: " + str(errtxt))

        ## Check if the object exists in Racktables
        objects = filter(lambda obj: obj['name'].lower() == objtosync['name'].lower(), objects)

        ## Object does not exist
        if len(objects) == 0:
            self.addObj(objtosync, objtype_id, linkparent)

        ## Objects exists
        if len(objects) == 1:
            targetObj = self.buildObjTree(objects[0])
            self.syncObj(targetObj, objtosync)

        ## Multiple objects exist
        if len(objects) > 1:
            logging.exception("More than one matching objects found.")

## -----------------------------------------
## Build object tree
## -----------------------------------------
    def buildObjTree(self, obj):
        logging.debug("Object found in Racktables. Building sync tree")

        targetObject = {}
        targetObject["name"]    = obj["name"]
        targetObject["id"]      = obj["id"]
        targetObject["attrs"]   = {}
        targetObject["network"] = {}
        

        try:
            ## get some more info and build the sync tree
            targetObjectInfos = self.rtClient.get_object(obj["id"], True, True)
        except Exception, errtxt:
            logging.exception("Error getting objects details: " + str(errtxt))
            raise errtxt

        ## add network ports
        for net_idx, net in targetObjectInfos['ports'].items():
            logging.debug("Processing port " + net['name'])

            targetObject["network"][net["name"]] = {
                "name": net["name"],
                "mac":  net["l2address"],
                "fqdn": net["label"],
                "id":   net["id"]
            }

            for ip_idx, ip in targetObjectInfos['ipv4'].items():
                # Raise if more than one allocation for current IP
                if len(ip["addrinfo"]["allocs"]) != 1:
                    raise Exception("Multiple allocations for IP:" + ip["addrinfo"]["ip"])
                
                if ip["osif"] == net["name"]:
                    targetObject["network"][net["name"]]["ip"] = ip["addrinfo"]["ip"]

        ## add attributes
        for attr_idx, attr in targetObjectInfos['attrs'].items():
            targetObject['attrs'][attr['id']] = attr['value']

        ## return the object
        return targetObject


## -----------------------------------------
## Add object
## -----------------------------------------
    def addObj(self, objtosync, objtype_id, linkparent):
        logging.debug("Object NOT found in Racktables. Creating it.")
        try:
            if 'attrs' in objtosync:
                newObject = self.rtClient.add_object(objtosync['name'], None ,objtosync['name'], objtype_id, None, [], objtosync['attrs'])
            else:
                newObject = self.rtClient.add_object(objtosync['name'], None ,objtosync['name'], objtype_id)
        except Exception, errtxt:
            logging.exception("Error adding object: " + str(errtxt))
            raise errtxt

        ## Delete all implicitly created ports
        logging.debug("Dropping all implicitly created ports")
        try:
            for port_idx, port in newObject['ports'].items():
                self.rtClient.delete_object_port(newObject['id'], port['id'])
        except Exception, errtxt:
            logging.exception("Error dropping ports: " + str(errtxt))

        ## Add ports    
        logging.debug("Adding network ports")
        try:
            for net_idx, net in objtosync['network'].items():
                net['newid'] = self.rtClient.add_object_port(newObject['id'], net['name'], net['mac'], '1-24', net['fqdn'])
                if 'ip' in net:
                    self.rtClient.add_object_ipv4_address(newObject['id'], net['ip'], net['name'])
        except Exception, errtxt:
            logging.exception("Error adding network port: " + str(errtxt))

        if linkparent:
            try:
                self.rtClient.link_entities(newObject['id'], linkparent)
            except Exception, errtxt:
                logging.exception("Error linking entities: " + str(errtxt))

## -----------------------------------------
## Sync object
## -----------------------------------------

    def syncObj(self, targetObject, objtosync):
        logging.debug("The object does exist, sync it") 

        ## Synchronize network ports
        for o_netidx, o_net in targetObject['network'].items():
            ## Loop through the ports assigned on Racktables
            ## If the port is found in the objtosync, deletePort is set to False and the port is synchronized
            ## Otherwise the port is deleted
            deletePort = True
            for n_netidx, n_net in objtosync['network'].items():
                ## If force is not explicitly set, assume no-force
                if not 'force' in n_net:
                    n_net['force'] = False

                ## Loop through the ports to sync
                if ( 
                        n_net['name'].lower() == o_net['name'].lower() and 
                        n_net['mac'].lower()  == o_net['mac'].lower()
                   ):
                    logging.debug('Port match found. Not deleting port.')
                    ## Port found, do not delete port
                    deletePort = False

                    if 'fqdn' in n_net:
                        ## If objtosync specifies a fqdn, check if it differs and force is true, then sync
                        if n_net['fqdn'].lower() != o_net['fqdn'].lower() and n_net['force'] == True:
                            logging.debug('fqdn differs. Updating from ' + o_net['fqdn'] + ' to ' + n_net['fqdn'])
                            self.rtClient.delete_object_port(targetObject['id'], o_net['id'])
                            self.rtClient.add_object_port(targetObject['id'], n_net['name'], n_net['mac'], '1-24', n_net['fqdn'])
                    if 'ip' in n_net:
                        ## Only handle IPs for this port if specified by the objtosync
                        if 'ip' in o_net:
                            ## if there is an IP set in Racktables, we need to sync it (if force is true)
                            if n_net['ip'] != o_net['ip'] and n_net['ip'] != '' and n_net['force'] == True:
                                logging.debug('IP differs. Updating from ' + o_net['ip'] + ' to ' + n_net['ip'])
                                logging.debug("Removing " + str(o_net['ip'] + " to add the new IP"))
                                self.rtClient.delete_object_ipv4_address(targetObject['id'], o_net['ip'])
                                logging.debug("Adding IP " + n_net['ip'])
                                self.rtClient.add_object_ipv4_address(targetObject['id'], n_net['ip'], n_net['name'])
                        else:
                            ## if there is no IP set in Racktables, we can simply add it
                            logging.debug("Adding IP " + n_net['ip'])
                            self.rtClient.add_object_ipv4_address(targetObject['id'], n_net['ip'], n_net['name'])
                    ## Interface is matched and updated, delete it from the sync object
                    logging.debug("Interface " + n_net['name'] + " is synchronized.")
                    del objtosync['network'][n_netidx]
            if deletePort == True:
                logging.debug('Port not found in target object. Deleting port.')
                ## if the racktables port is not found in the sync object, delete it
                if 'ip' in o_net:
                    self.rtClient.delete_object_ipv4_address(targetObject['id'], o_net['ip'])
                else:
                    logging.debug('No IP Address found for port.')
                self.rtClient.delete_object_port(targetObject['id'], o_net['id'])

        ## Now add the ports we're left with
        try:
            for net_idx, net in objtosync['network'].items():
                net['newid'] = self.rtClient.add_object_port(objtosync['id'], net['name'], net['mac'], '1-24', net['fqdn'])
                try:
                    if 'ip' in net:
                        self.rtClient.add_object_ipv4_address(objtosync['id'], net['ip'], net['name'])
                except KeyError:
                    logging.debug("No IP found for interface " + net['name'])
                    continue
        except Exception, errtxt:
            logging.exception("Error adding network port: " + str(errtxt))

        if not "attrs" in objtosync:
            return

        ## Synchronize attrs
        for o_attrname, o_attrval in targetObject["attrs"].items():
            if o_attrname in objtosync["attrs"] and o_attrname in objtosync["forceattrs"]:
                continue

            objtosync['attrs'][o_attrname] = o_attrval
            

        logging.debug("Updating attributes")
        
        try:
            self.rtClient.edit_object(targetObjectInfos['id'], targetObjectInfos['name'], targetObjectInfos['asset_no'], targetObjectInfos['label'], targetObjectInfos['objtype_id'], targetObjectInfos['comment'], objtosync['attrs'])
        except Exception, errtxt:
            logging.exception("Error writing back attributes: %s" % errtxt)
