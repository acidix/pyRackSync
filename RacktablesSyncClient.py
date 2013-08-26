#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Python Module to push (one way) to Racktables

import logging

class RacktablesSyncClient:

	def __init__(self, RackTablesClient):
		self.rtClient = RackTablesClient

	def syncServer (self, objtosync, objtype_id='4', linkparent=0):
		try:
			objects = {}
			objects = self.rtClient.get_objects()
		except Exception, errtxt:
			logging.exception("Error initializing object tree: " + str(errtxt))

		targetObject = {}

		try:
			for object_idx, object in objects.items():
				# check it has the right name & objtype_id
				if ( 
					object['name'].lower() == objtosync['name'].lower() and object['objtype_id'] == objtype_id 
					# or if network interface matches mac - then rename the host
				   ):
					logging.debug("Object found in Racktables. Building sync tree")
					targetObject['name'] = object['name']
					targetObject['id'] = object['id']
					targetObject['network'] = {}
					targetObject['attrs'] = {}
	
					try:
						# get some more info and build the sync tree
						targetObjectInfos = {}
						targetObjectInfos = self.rtClient.get_object(object['id'], True, True)
					except Exception, errtxt:
						logging.exception("Error getting objects details: " + str(errtxt))
	
					try:
						# add network ports
						for net_idx, net in targetObjectInfos['ports'].items():
							logging.debug("Processing port " + net['name'])
							targetObject['network'][net['name']] = {}
							targetObject['network'][net['name']]['name'] = net['name']
							targetObject['network'][net['name']]['mac'] = net['l2address']
							targetObject['network'][net['name']]['fqdn'] = net['label']
							targetObject['network'][net['name']]['id'] = net['id']
		
							try:
								for ip_idx, ip in targetObjectInfos['ipv4'].items():
									for ipalloc_idx, ipalloc in ip['addrinfo']['allocs'].items():
										if ipalloc['object_id'] == object['id'] and ipalloc['name'] == net['name']:
											targetObject['network'][net['name']]['ip'] = ip['addrinfo']['ip']
							except Exception, errtxt:
								logging.exception("Error finding IPv4 address for port: " + str(errtxt))

						# add attributes
						for attr_idx, attr in targetObjectInfos['attrs'].items():
							try:
								targetObject['attrs'][attr['id']] = attr['value']
							except Exception, errtxt:
								logging.exception("Error adding attribute: " + str(errtxt))

					except Exception, errtxt:
						logging.exception("Error looping through ports tree: " + str(errtxt))
		except Exception, errtxt:
			logging.exception("Error looping through object tree: " + str(errtxt))

# ------------------------------------------------------------------------------------------------------------
# Add object
		if not targetObject:
			# the object doesn't exist create it
			logging.debug("Object NOT found in Racktables. Creating it.")
			try:
				try:
					if 'attrs' in objtosync:
						newObject = self.rtClient.add_object(objtosync['name'], None ,objtosync['name'], objtype_id, None, [], objtosync['attrs'])
					else:
						newObject = self.rtClient.add_object(objtosync['name'], None ,objtosync['name'], objtype_id)
				except KeyError:
					newObject = self.rtClient.add_object(objtosync['name'], None ,objtosync['name'], objtype_id)	
			except Exception. errtxt:
				logging.exception("Error adding object: " + str(errtxt))

			# delete all implicitly created ports
			logging.debug("Dropping all implicitly created ports")
			try:
				for port_idx, port in newObject['ports'].items():
					self.rtClient.delete_object_port(newObject['id'], port['id'])
			except Exception, errtxt:
				logging.exception("Error dropping ports: " + str(errtxt))

			# add ports	
			logging.debug("Adding network ports")
			try:
				for net_idx, net in objtosync['network'].items():
					net['newid'] = self.rtClient.add_object_port(newObject['id'], net['name'], net['mac'], '1-24', net['fqdn'])
					try:
						if 'ip' in net:
							self.rtClient.add_object_ipv4_address(newObject['id'], net['ip'], net['name'])
					except KeyError:
						logging.debug("No IP found for interface " + net['name'])
						continue
			except Exception, errtxt:
				logging.exception("Error adding network port: " + str(errtxt))

			if linkparent != 0:
				try:
					self.rtClient.link_entities(newObject['id'], linkparent)
				except Exception, errtxt:
					logging.exception("Error linking entities: " + str(errtxt))

# ------------------------------------------------------------------------------------------------------------
# Sync object
		else:
			logging.debug("The object does exist, sync it") 

			# Delete network ports if neccessary
			for o_netidx, o_net in targetObject['network'].items():
				# loop through the ports assigned on Racktables
				deletePort = True
				for n_netidx, n_net in objtosync['network'].items():
					# loop through the ports to sync
					if ( 
							n_net['name'].lower() == o_net['name'].lower() and 
							n_net['mac'].lower()  == o_net['mac'].lower()
					   ):
						logging.debug('Port match found. Not deleting port.')
						# do not delete port
						deletePort = False

						if 'fqdn' in n_net:
							if n_net['fqdn'].lower() != o_net['fqdn'].lower():
								logging.debug('fqdn differs. Updating from ' + o_net['fqdn'] + ' to ' + n_net['fqdn'])
								self.rtClient.delete_object_port(targetObject['id'], o_net['id'])
								self.rtClient.add_object_port(targetObject['id'], n_net['name'], n_net['mac'], '1-24', n_net['fqdn'])
						if 'ip' in n_net:
							if 'ip' in o_net:
								if n_net['ip'] != o_net['ip'] and n_net['ip'] != '':
									logging.debug('IP differs. Updating from ' + o_net['ip'] + ' to ' + n_net['ip'])
									logging.debug("Removing " + str(o_net['ip'] + " to add the new IP"))
									self.rtClient.delete_object_ipv4_address(targetObject['id'], o_net['ip'])
									logging.debug("Adding IP " + n_net['ip'])
									self.rtClient.add_object_ipv4_address(targetObject['id'], n_net['ip'], n_net['name'])
							else:
								logging.debug("Adding IP " + n_net['ip'])
								self.rtClient.add_object_ipv4_address(targetObject['id'], n_net['ip'], n_net['name'])
						# interface is matched and updated, delete it from the sync object
						logging.debug("Interface " + n_net['name'] + " is synchronized.")
						del objtosync['network'][n_netidx]
				if deletePort == True:
					logging.debug('Port not found in target object. Deleting port.')
					# delete the port
					if 'ip' in o_net:
						self.rtClient.delete_object_ipv4_address(targetObject['id'], o_net['ip'])
					else:
						logging.debug('No IP Address found for port.')
					self.rtClient.delete_object_port(targetObject['id'], net['id'])

			# Now add the ports we're left with
			try:
				for net_idx, net in objtosync['network'].items():
					net['newid'] = self.rtClient.add_object_port(newObject['id'], net['name'], net['mac'], '1-24', net['fqdn'])
					try:
						if 'ip' in net:
							self.rtClient.add_object_ipv4_address(newObject['id'], net['ip'], net['name'])
					except KeyError:
						logging.debug("No IP found for interface " + net['name'])
						continue
			except Exception, errtxt:
				logging.exception("Error adding network port: " + str(errtxt))


			if 'attrs' in objtosync:
				# Synchronize attrs
				for o_attridx, o_attrval in targetObject['attrs'].items():
						if not o_attridx in objtosync['attrs']:
							logging.debug("Adding attribute to target attribute vector")
							objtosync['attrs'][o_attridx] = o_attrval
	
				logging.debug("Updating attributes")
				self.rtClient.edit_object(targetObjectInfos['id'], targetObjectInfos['name'], targetObjectInfos['asset_no'], targetObjectInfos['label'], targetObjectInfos['objtype_id'], targetObjectInfos['comment'], objtosync['attrs'])
