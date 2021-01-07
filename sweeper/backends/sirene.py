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
from datetime import datetime, date

import xmltodict
import requests
from requests.auth import HTTPBasicAuth

from sweeper.backends.base import BaseBackend
from sweeper.gateways.ssh import SSHGateway
from sweeper.gateways.http import HTTPDownloadGateway
from sweeper.gateways.datagouvfr import DataGouvFrGateway
from sweeper.models import Resource


class SireneBackend(BaseBackend):
    name = "sirene"

    def pre_run(self):
        pass

    def run(self):
        source_url = self.config["source_url"]
        auth = None
        if self.secrets["basicauth_user"]:
            auth = HTTPBasicAuth(
                self.secrets["basicauth_user"],
                self.secrets["basicauth_password"],
            )
        r = requests.get(source_url, auth=auth)
        assert r.status_code == 200, "bad response from list"
        xmldict = xmltodict.parse(r.text)
        files = xmldict['ns2:ServiceDepotRetrait']['Fichiers']
        if not isinstance(files, list):
            files = [files]

        downloader = HTTPDownloadGateway(self.file_has_changed, self.tmp_dir, auth=auth)

        for file in files:
            try:
                if file["id"] not in self.config["mapping"]:
                    print(f"{file['id']} not found in mapping")
                    continue
                has_changed, infos = downloader.download(file["URI"], file["id"])
                if has_changed:
                    self.upload(infos)
                else:
                    print(f"{file['id']} has not changed.")
            except Exception as e:
                self.register_error(Resource(name=file["id"], error=str(e)))
                # FIXME:
                raise
                continue

    def upload(self, resource: Resource):
        uploader = SSHGateway(self.config["destination_host"])
        datagouvfr = DataGouvFrGateway(self.secrets["datagouvfr_token"], demo=self.config["demo"])
        try:
            # uploading to files.data.gouv.fr
            remote = f"{self.config['destination_dir']}/{resource.name}"
            uploader.upload(resource.file, remote)
            today = date.today().isoformat()
            remote_date = f"{self.config['destination_dir']}/{today}-{resource.name}"
            uploader.upload(resource.file, remote_date)
            # update datagouvfr
            title = resource.name.replace("_utf8.zip", "")
            res = datagouvfr.remote_replace_resource(
                self.config["dataset_id"],
                self.config["mapping"][resource.name],
                f"https://files.data.gouv.fr/insee-sirene/{resource.name}",
                f"Fichier {title} du {datetime.now().strftime('%d %B %Y')}",
                filesize=resource.size,
                checksum={"value": resource.sha1sum, "type": "sha1"},
            )
            # keep track in DB
            self.register_file(resource)
            return res
        finally:
            uploader.teardown()

    def post_run(self):
        pass
