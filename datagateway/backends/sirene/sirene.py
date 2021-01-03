"""Get files from INSEE SIRENE HTTP 'API'

<?xml version="1.0" encoding="UTF-8"?>
<ns2:ServiceDepotRetrait xmlns:ns2="http://xml.insee.fr/schema/outils">
   <Fichiers>
      <id>StockUniteLegaleHistorique_utf8.zip</id>
      <URI>https://echanges.insee.fr/ressources/apisir-etalab/fichier/StockUniteLegaleHistorique_utf8.zip</URI>
   </Fichiers>
   <Fichiers>
      <id>StockEtablissement_utf8.zip</id>
      <URI>https://echanges.insee.fr/ressources/apisir-etalab/fichier/StockEtablissement_utf8.zip</URI>
   </Fichiers>
   <Fichiers>
      <id>StockEtablissementHistorique_utf8.zip</id>
      <URI>https://echanges.insee.fr/ressources/apisir-etalab/fichier/StockEtablissementHistorique_utf8.zip</URI>
   </Fichiers>
   <Fichiers>
      <id>StockUniteLegale_utf8.zip</id>
      <URI>https://echanges.insee.fr/ressources/apisir-etalab/fichier/StockUniteLegale_utf8.zip</URI>
   </Fichiers>
   <Fichiers>
      <id>StockEtablissementLiensSuccession_utf8.zip</id>
      <URI>https://echanges.insee.fr/ressources/apisir-etalab/fichier/StockEtablissementLiensSuccession_utf8.zip</URI>
   </Fichiers>
</ns2:ServiceDepotRetrait>
"""
from datetime import datetime

import xmltodict
import requests
from requests.auth import HTTPBasicAuth

from datagateway.backends.base import BaseBackend
from datagateway.gateways.ssh import SSHGateway
from datagateway.gateways.http import HTTPDownloadGateway


class SireneBackend(BaseBackend):
    name = "sirene"

    def pre_run(self):
        self.tmp_files = []

    def run(self):
        source_url = self.config["source_url"]
        auth = HTTPBasicAuth(
            self.secrets["basicauth_user"],
            self.secrets["basicauth_password"],
        )
        r = requests.get(source_url, auth=auth)
        assert r.status_code == 200, "bad response from list"
        xmldict = xmltodict.parse(r.text)
        files = xmldict['ns2:ServiceDepotRetrait']['Fichiers']

        downloader = HTTPDownloadGateway(self.table, self.tmp_dir, auth=auth)

        # TODO: continue on error
        for file in files:
            has_changed, infos = downloader.download(file["URI"], file["id"])
            if has_changed:
                self.upload(infos)
            else:
                print(f"{file['id']} has not changed.")

    def upload(self, resource):
        uploader = SSHGateway("maboiteprivee.fr")
        try:
            print(f"Uploading {resource['name']}...")
            remote = f"/root/data-gw/{resource['name']}"
            uploader.upload(resource["file"], remote)
            resource.pop("file")
            resource["created_at"] = datetime.utcnow()
            self.table.insert(resource)
        finally:
            uploader.teardown()

    def post_run(self):
        pass
