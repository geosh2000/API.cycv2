<?php

class mailSolicitudPuesto{

  public $destinatario;
  public $mailInfo;

  public static function mail( $class, $params, $vac_off, $tipo ){

    $mailInfo['asesor'] = $params['asesor'];
    $mailInfo['fechaCambio'] = $params['fechaCambio'];
    $mailInfo['fechaLiberacion'] = $params['fechaLiberacion'];
    $mailInfo['reemplazable'] = (int)$params['reemplazable'];
    $mailInfo['applier'] = $params['applier'];
    $mailInfo['old']['vacante'] = $vac_off;
    $mailInfo['new']['vacante'] = $params['puesto']['vacante'];

    if(isset($params['approber'])){
      $mailInfo['approber'] = $params['approber'];
    }else{
      $mailInfo['approber'] = 0;
    }

    if(isset($params['action'])){
      $mailInfo['action'] = $params['action'];
    }else{
      $mailInfo['action'] = false;
    }

    $query="SELECT
                a.id, f.Departamento, c.Puesto, PDV, f.UDN, f.Area, f.Puesto as PuestoRRHH, CodigoPuesto
            FROM
                asesores_plazas a
                    LEFT JOIN
                PCRCs b ON a.departamento = b.id
                    LEFT JOIN
                PCRCs_puestos c ON a.puesto = c.id
                    LEFT JOIN
                PDVs e ON a.oficina = e.id
                    LEFT JOIN
                (SELECT
                    a.id AS puestoID,
                        d.nombre AS UDN,
                        c.nombre AS Area,
                        b.nombre AS Departamento,
                        a.nombre AS Puesto,
                        CONCAT(d.clave,'-',c.clave,'-',b.clave,'-',a.clave) as CodigoPuesto
                FROM
                    hc_codigos_Puesto a
                LEFT JOIN hc_codigos_Departamento b ON a.departamento = b.id
                LEFT JOIN hc_codigos_Areas c ON b.area = c.id
                LEFT JOIN hc_codigos_UnidadDeNegocio d ON c.unidadDeNegocio = d.id) f ON a.hc_puesto = f.puestoID
            WHERE
                a.id IN (".$mailInfo['old']['vacante'].",".$mailInfo['new']['vacante'].")";

    $q = $class->db->query($query);
    foreach ($q->result() as $row){
      if($row->id == $mailInfo['old']['vacante']){
        $flag = 'old';
      }else{
        $flag = 'new';
      }

      $mailInfo[$flag]['dep']=$row->Departamento;
      $mailInfo[$flag]['puesto']=$row->Puesto;
      $mailInfo[$flag]['puestoRH']=$row->PuestoRRHH;
      $mailInfo[$flag]['oficina']=$row->PDV;
      $mailInfo[$flag]['udn']=$row->UDN;
      $mailInfo[$flag]['area']=$row->Area;
      $mailInfo[$flag]['codigo']=$row->CodigoPuesto;
    }

    $query="SELECT NombreAsesor(".$mailInfo['asesor'].",2) as nombreAsesor, NombreAsesor(".$mailInfo['applier'].",1) as nombreSol, NombreAsesor(".$mailInfo['approber'].",1) as nombreAprobador";
    $q = $class->db->query($query);
    $result = $q->row();
    $mailInfo['nombreAsesor'] = $result->nombreAsesor;
    $mailInfo['nombreAprobador'] = $result->nombreAprobador;
    $mailInfo['sol'] = $result->nombreSol;


    if($tipo == 'ask'){
      $mailList="cambio_puestoSOL";
    }else{
      $mailList="cambio_puestoOK";
      mailSolicitudPuesto::sendMail(str_replace(" ",".",strtolower($mailInfo['sol'])),$mailInfo,$tipo);
    }

    $query="SELECT usuario FROM mail_lists WHERE notif='$mailList'";
    $q = $class->db->query($query);
    foreach ($q->result() as $row){
      mailSolicitudPuesto::sendMail($row->usuario,$mailInfo,$tipo);
    }

  }

  public static function sendMail($user, $m_data, $tipo){
    $name=str_replace('.',' ',$user);
    $name=ucwords($name);

    if($tipo == 'ask'){
      $title="Solicitud de Camio de Puesto";
      $descript=$m_data['sol']."</strong> ha registrado una <strong class=\"font-weight-bold\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;font-weight: 700;\">solicitud de cambio</strong> en el ComeyCome.";
      $option="<a class=\"btn btn-primary btn-lg\" href=\"https://operaciones.pricetravel.com.mx/cycv2/#/aprobaciones_rrhh\" role=\"button\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;background-color: #0275d8;-webkit-text-decoration-skip: objects;color: #fff;text-decoration: underline;-ms-touch-action: manipulation;touch-action: manipulation;cursor: pointer;display: inline-block;font-weight: 400;line-height: 1.25;text-align: center;white-space: nowrap;vertical-align: middle;-webkit-user-select: none;-moz-user-select: none;-ms-user-select: none;user-select: none;border: 1px solid transparent;padding: .75rem 1.5rem;font-size: 1.25rem;border-radius: .3rem;-webkit-transition: all .2s ease-in-out;-o-transition: all .2s ease-in-out;transition: all .2s ease-in-out;border-color: #0275d8;\">Ver Solicitud</a>\n";
      $mailTitle="Solicitud de Cambio de Puesto (".$m_data['nombreAsesor'].")";
    }else{
      if($m_data['action']){
        $title="Camio de Puesto Aprobado";
        $descript=$m_data['nombreAprobador']."</strong> ha <strong class=\"font-weight-bold\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;font-weight: 700;\">aprobado</strong> el cambio de puesto solicitado.";
        $option="<span class=\"btn btn-success btn-lg\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;display: inline-block;font-weight: 400;line-height: 1.25;text-align: center;white-space: nowrap;vertical-align: middle;-webkit-user-select: none;-moz-user-select: none;-ms-user-select: none;user-select: none;border: 1px solid transparent;padding: .75rem 1.5rem;font-size: 1.25rem;border-radius: .3rem;-webkit-transition: all .2s ease-in-out;-o-transition: all .2s ease-in-out;transition: all .2s ease-in-out;color: #fff;background-color: #5cb85c;border-color: #5cb85c;\">Aprobada</span>\n";
        $mailTitle="Cambio de Puesto Aprobado (".$m_data['nombreAsesor'].")";
      }else{
        $title="Cambio de Puesto Declinado";
        $descript=$m_data['nombreAprobador']."</strong> ha <strong class=\"font-weight-bold\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;font-weight: 700;\">declinado</strong> el cambio de puesto solicitado.";
        $option="<span class=\"btn btn-danger btn-lg\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;display: inline-block;font-weight: 400;line-height: 1.25;text-align: center;white-space: nowrap;vertical-align: middle;-webkit-user-select: none;-moz-user-select: none;-ms-user-select: none;user-select: none;border: 1px solid transparent;padding: .75rem 1.5rem;font-size: 1.25rem;border-radius: .3rem;-webkit-transition: all .2s ease-in-out;-o-transition: all .2s ease-in-out;transition: all .2s ease-in-out;color: #fff;background-color: #d9534f;border-color: #d9534f;\">Denegada</span>\n";
        $mailTitle="Cambio de Puesto Declinado (".$m_data['nombreAsesor'].")";
      }


    }

    $msg= "<html xmlns=\"http://www.w3.org/1999/xhtml\" style=\"-webkit-box-sizing: border-box;box-sizing: border-box;font-family: sans-serif;line-height: 1.15;-ms-text-size-adjust: 100%;-webkit-text-size-adjust: 100%;-ms-overflow-style: scrollbar;-webkit-tap-highlight-color: transparent;\">\n";
    $msg.= "  <head style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">\n";
    $msg.= "    <meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">\n";
    $msg.= "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">\n";
    $msg.= "    <title style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">$title</title>\n";
    $msg.= "  </head>\n";
    $msg.= "  <body style=\"-webkit-box-sizing: inherit;box-sizing: inherit;margin: 0;font-family: -apple-system,system-ui,BlinkMacSystemFont,&quot;Segoe UI&quot;,Roboto,&quot;Helvetica Neue&quot;,Arial,sans-serif;font-size: 1rem;font-weight: 400;line-height: 1.5;color: #292b2c;background-color: #fff;\">\n";
    $msg.= "    <div class=\"container\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;margin-left: auto;margin-right: auto;padding-right: 15px;padding-left: 15px;  min-width: 900px; max-width:1200px\">\n";
    $msg.= "      <div class=\"card text-center\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-box-orient: vertical;-webkit-box-direction: normal;-webkit-flex-direction: column;-ms-flex-direction: column;flex-direction: column;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-radius: .25rem;text-align: center!important;\">\n";
    $msg.= "        <div class=\"card-header\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;padding: .75rem 1.25rem;margin-bottom: 0;background-color: #f7f7f9;border-bottom: 1px solid rgba(0,0,0,.125);border-radius: calc(.25rem - 1px) calc(.25rem - 1px) 0 0;\">\n";
    $msg.= "          ComeyCome\n";
    $msg.= "        </div>\n";
    $msg.= "        <div class=\"card-block\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;-webkit-box-flex: 1;-webkit-flex: 1 1 auto;-ms-flex: 1 1 auto;flex: 1 1 auto;padding: 1.25rem;\">\n";
    $msg.= "          <h4 class=\"card-title\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;margin-top: 0;margin-bottom: .75rem;font-family: inherit;font-weight: 500;line-height: 1.1;color: inherit;font-size: 1.5rem;\">$title</h4>\n";
    $msg.= "          <p class=\"card-text\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;orphans: 3;widows: 3;margin-top: 0;margin-bottom: 1rem;\">Hola $name!, <strong class=\"font-weight-bold\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;font-weight: 700;\">".$descript." A continuación los detalles</p>\n";
    $msg.= "          <hr class=\"my-4\" style=\"-webkit-box-sizing: content-box;box-sizing: content-box;height: 0;overflow: visible;margin-top: 1.5rem!important;margin-bottom: 1.5rem!important;border: 0;border-top: 1px solid rgba(0,0,0,.1);\">\n";
    $msg.= "          <div class=\"d-flex justify-content-around align-items-center\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;display: flex!important;-webkit-justify-content: space-around!important;-ms-flex-pack: distribute!important;justify-content: space-around!important;-webkit-box-align: center!important;-webkit-align-items: center!important;-ms-flex-align: center!important;align-items: center!important;\">\n";
    $msg.= "\n";
    $msg.= "            <!-- OLD Puesto -->\n";
    $msg.= "            <div class=\"p-2 card\" style=\"width: 20rem;-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-box-orient: vertical;-webkit-box-direction: normal;-webkit-flex-direction: column;-ms-flex-direction: column;flex-direction: column;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-radius: .25rem;padding: .5rem .5rem!important;\">\n";
    $msg.= "              <div class=\"card-block\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;-webkit-box-flex: 1;-webkit-flex: 1 1 auto;-ms-flex: 1 1 auto;flex: 1 1 auto;padding: 1.25rem;\">\n";
    $msg.= "                <h4 class=\"card-title\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;margin-top: 0;margin-bottom: .75rem;font-family: inherit;font-weight: 500;line-height: 1.1;color: inherit;font-size: 1.5rem;\">Puesto Actual</h4>\n";
    $msg.= "              </div>\n";
    $msg.= "              <ul class=\"list-group list-group-flush\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;margin-top: 0;margin-bottom: 0;display: flex;-webkit-box-orient: vertical;-webkit-box-direction: normal;-webkit-flex-direction: column;-ms-flex-direction: column;flex-direction: column;padding-left: 0;\">\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-top-right-radius: .25rem;border-top-left-radius: .25rem;border-right: 0;border-left: 0;border-radius: 0;\">".utf8_encode($m_data['old']['udn'])."</li>\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-top-right-radius: .25rem;border-top-left-radius: .25rem;border-right: 0;border-left: 0;border-radius: 0;\">".utf8_encode($m_data['old']['area'])."</li>\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-top-right-radius: .25rem;border-top-left-radius: .25rem;border-right: 0;border-left: 0;border-radius: 0;\">".utf8_encode($m_data['old']['dep'])."</li>\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-right: 0;border-left: 0;border-radius: 0;\">";
    $msg.= "                ".utf8_encode($m_data['old']['puesto'])." (".utf8_encode($m_data['old']['puestoRH']).")</li>\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-right: 0;border-left: 0;border-radius: 0;\">";
    $msg.= "                ".utf8_encode($m_data['old']['codigo'])."</li>\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: 0;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-bottom-right-radius: .25rem;border-bottom-left-radius: .25rem;border-right: 0;border-left: 0;border-radius: 0;\">".$m_data['old']['oficina']."</li>\n";
    $msg.= "              </ul>\n";
    $msg.= "              <div class=\"card-block\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;-webkit-box-flex: 1;-webkit-flex: 1 1 auto;-ms-flex: 1 1 auto;flex: 1 1 auto;padding: 1.25rem;\">\n";

    if( $m_data['reemplazable'] == 1 ){
      $reemplazable="La vacante anterior quedaría liberada con fecha:";
      $fechaReemplazo=$m_data['fechaLiberacion'];
    }else{
      $reemplazable="La vacante anterior quedaría inactiva por lo que no necesita reemplazo";
      $fechaReemplazo="";
    }

    $msg.= "                <p>$reemplazable</p>";
    $msg.= "                <p>$fechaReemplazo</p>";
    $msg.= "              </div>\n";
    $msg.= "            </div>\n";
    $msg.= "            <div class=\"p-2 text-center\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;padding: .5rem .5rem!important;text-align: center!important;\">\n";
    $msg.= "              <h3 style=\"-webkit-box-sizing: inherit;box-sizing: inherit;orphans: 3;widows: 3;page-break-after: avoid;margin-top: 0;margin-bottom: .5rem;font-family: inherit;font-weight: 500;line-height: 1.1;color: inherit;font-size: 1.75rem;\">".$m_data['nombreAsesor']."</h3>\n";
    $msg.= "              <h1 style=\"-webkit-box-sizing: inherit;box-sizing: inherit;font-size: 2.5rem;margin: .67em 0;margin-top: 0;margin-bottom: .5rem;font-family: inherit;font-weight: 500;line-height: 1.1;color: inherit;\">=></h1>\n";
    $msg.= "              <h3 style=\"-webkit-box-sizing: inherit;box-sizing: inherit;orphans: 3;widows: 3;page-break-after: avoid;margin-top: 0;margin-bottom: .5rem;font-family: inherit;font-weight: 500;line-height: 1.1;color: inherit;font-size: 1.75rem;\">".$m_data['fechaCambio']."</h3>\n";
    $msg.= "            </div>\n";
    $msg.= "\n";
    $msg.= "            <!-- NEW Puesto -->\n";
    $msg.= "            <div class=\"p-2 card\" style=\"width: 20rem;-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-box-orient: vertical;-webkit-box-direction: normal;-webkit-flex-direction: column;-ms-flex-direction: column;flex-direction: column;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-radius: .25rem;padding: .5rem .5rem!important;\">\n";
    $msg.= "              <div class=\"card-block\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;-webkit-box-flex: 1;-webkit-flex: 1 1 auto;-ms-flex: 1 1 auto;flex: 1 1 auto;padding: 1.25rem;\">\n";
    $msg.= "                <h4 class=\"card-title\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;margin-top: 0;margin-bottom: .75rem;font-family: inherit;font-weight: 500;line-height: 1.1;color: inherit;font-size: 1.5rem;\">Puesto Solicitado</h4>\n";
    $msg.= "              </div>\n";
    $msg.= "              <ul class=\"list-group list-group-flush\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;margin-top: 0;margin-bottom: 0;display: flex;-webkit-box-orient: vertical;-webkit-box-direction: normal;-webkit-flex-direction: column;-ms-flex-direction: column;flex-direction: column;padding-left: 0;\">\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-top-right-radius: .25rem;border-top-left-radius: .25rem;border-right: 0;border-left: 0;border-radius: 0;\">".utf8_encode($m_data['new']['udn'])."</li>\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-top-right-radius: .25rem;border-top-left-radius: .25rem;border-right: 0;border-left: 0;border-radius: 0;\">".utf8_encode($m_data['new']['area'])."</li>\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-top-right-radius: .25rem;border-top-left-radius: .25rem;border-right: 0;border-left: 0;border-radius: 0;\">".utf8_encode($m_data['new']['dep'])."</li>\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-right: 0;border-left: 0;border-radius: 0;\">";
    $msg.= "                ".utf8_encode($m_data['new']['puesto'])." (".utf8_encode($m_data['new']['puestoRH']).")</li>\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-right: 0;border-left: 0;border-radius: 0;\">";
    $msg.= "                ".utf8_encode($m_data['new']['codigo'])."</li>\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: 0;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-bottom-right-radius: .25rem;border-bottom-left-radius: .25rem;border-right: 0;border-left: 0;border-radius: 0;\">".$m_data['new']['oficina']."</li>\n";
    $msg.= "              </ul>\n";
    $msg.= "              <div class=\"card-block\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;-webkit-box-flex: 1;-webkit-flex: 1 1 auto;-ms-flex: 1 1 auto;flex: 1 1 auto;padding: 1.25rem;\">\n";
    $msg.= "                <p class=\"text-center\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;orphans: 3;widows: 3;margin-top: 0;margin-bottom: 1rem;text-align: center!important;\">\n";
    $msg.= "                  $option";
    $msg.= "                </p>\n";
    $msg.= "              </div>\n";
    $msg.= "            </div>\n";
    $msg.= "          </div>\n";
    $msg.= "          <hr class=\"my-4\" style=\"-webkit-box-sizing: content-box;box-sizing: content-box;height: 0;overflow: visible;margin-top: 1.5rem!important;margin-bottom: 1.5rem!important;border: 0;border-top: 1px solid rgba(0,0,0,.1);\">\n";
    $msg.= "        </div>\n";
    $msg.= "        <div class=\"card-footer text-muted\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;padding: .75rem 1.25rem;background-color: #f7f7f9;border-top: 1px solid rgba(0,0,0,.125);border-radius: 0 0 calc(.25rem - 1px) calc(.25rem - 1px);color: #636c72!important;\">\n";
    $msg.= "          © ComeyCome 2017. Todos los derechos reservados\n";
    $msg.= "        </div>\n";
    $msg.= "      </div>\n";
    $msg.= "\n";
    $msg.= "    </div>\n";
    $msg.= "  </body>\n";
    $msg.= "</html>\n";
    $msg.= "\n";

    $headers = "MIME-Version: 1.0" . "\r\n";
    $headers .= "Content-type:text/html;charset=UTF-8" . "\r\n";
    $headers .= "From: Notificaciones ComeyCome <operaciones@pricetravel.com>";

    mail("$user@pricetravel.com",$mailTitle ,$msg,$headers);

    // echo $msg;

  }


}


class mailSolicitudBaja{

  public static function mail( $class, $params, $vac_off, $tipo ){

    $mailInfo['asesor'] = $params['asesor'];
    $mailInfo['fechaBaja'] = $params['fecha'];
    $mailInfo['fechaLiberacion'] = $params['fecha_replace'];
    $mailInfo['reemplazable'] = (int)$params['reemplazable'];
    $mailInfo['recontratable'] = (int)$params['recontratable'];
    $mailInfo['comentarios'] = $params['comentarios'];
    if(isset($params['status'])){$mailInfo['status'] = $params['status'];}else{$mailInfo['status'] = 0;}
    if(isset($params['comentariosRRHH'])){
      $mailInfo['comentariosRRHH'] = $params['comentariosRRHH'];
    }

    if($tipo == 'ask'){
      $mailInfo['applier'] = $params['solicitado_por'];
    }else{
      if(isset($params['aprobado_por'])){
        $mailInfo['applier'] = $params['aprobado_por'];
      }else{
        $mailInfo['applier'] = $params['solicitado_por'];
      }

    }
    $mailInfo['old']['vacante'] = $vac_off;

    $query="SELECT
                a.id, f.Departamento, c.Puesto, PDV, f.UDN, f.Area, f.Puesto as PuestoRRHH, CodigoPuesto
            FROM
                asesores_plazas a
                    LEFT JOIN
                PCRCs b ON a.departamento = b.id
                    LEFT JOIN
                PCRCs_puestos c ON a.puesto = c.id
                    LEFT JOIN
                PDVs e ON a.oficina = e.id
                    LEFT JOIN
                (SELECT
                    a.id AS puestoID,
                        d.nombre AS UDN,
                        c.nombre AS Area,
                        b.nombre AS Departamento,
                        a.nombre AS Puesto,
                        CONCAT(d.clave,'-',c.clave,'-',b.clave,'-',a.clave) as CodigoPuesto
                FROM
                    hc_codigos_Puesto a
                LEFT JOIN hc_codigos_Departamento b ON a.departamento = b.id
                LEFT JOIN hc_codigos_Areas c ON b.area = c.id
                LEFT JOIN hc_codigos_UnidadDeNegocio d ON c.unidadDeNegocio = d.id) f ON a.hc_puesto = f.puestoID
            WHERE
                a.id IN (".$mailInfo['old']['vacante'].")";

    $q = $class->db->query($query);
    foreach ($q->result() as $row){
      if($row->id == $mailInfo['old']['vacante']){
        $flag = 'old';
      }else{
        $flag = 'new';
      }

      $mailInfo[$flag]['dep']=$row->Departamento;
      $mailInfo[$flag]['puesto']=$row->Puesto;
      $mailInfo[$flag]['puestoRH']=$row->PuestoRRHH;
      $mailInfo[$flag]['oficina']=$row->PDV;
      $mailInfo[$flag]['udn']=$row->UDN;
      $mailInfo[$flag]['area']=$row->Area;
      $mailInfo[$flag]['codigo']=$row->CodigoPuesto;
    }

    $query="SELECT NombreAsesor(".$mailInfo['asesor'].",2) as nombreAsesor, NombreAsesor(".$params['solicitado_por'].",1) as nombreSol, NombreAsesor(".$mailInfo['applier'].",1) as nombreApl";
    $q = $class->db->query($query);
    $result = $q->row();
    $mailInfo['nombreAsesor'] = $result->nombreAsesor;
    $mailInfo['sol'] = $result->nombreSol;
    $mailInfo['NameApplier'] = $result->nombreApl;

    if($tipo == 'ask'){
      $query="SELECT usuario FROM mail_lists WHERE notif='bajaSOL'";
      $q = $class->db->query($query);
      foreach ($q->result() as $row){
        mailSolicitudBaja::sendMail($row->usuario,$mailInfo, 'ask');
      }
    }else{
      $query="SELECT usuario FROM mail_lists WHERE notif='bajaOK'";
      $q = $class->db->query($query);
      foreach ($q->result() as $row){
        mailSolicitudBaja::sendMail($row->usuario,$mailInfo, 'set');
      }

      mailSolicitudBaja::sendMail(str_replace(' ','.',$mailInfo['sol']),$mailInfo, 'set');
    }

  }

  public static function sendMail($user, $m_data, $tipo){

    switch($tipo){
      case 'ask':
        $registerer = $m_data['sol'];
        $titulo = "Solicitud de Baja ".$m_data['nombreAsesor'];
        $action = "solicitud de baja";
        break;
      case 'set':
        $registerer = $m_data['NameApplier'];
        if($m_data['status']==1){
          $titulo = "Baja ".$m_data['nombreAsesor']." aprobada";
          $action = "aprobación de baja";
        }else{
          $titulo = "Baja ".$m_data['nombreAsesor']." denegada";
          $action = "denegación de baja";
        }
        break;
    }

    $name=str_replace('.',' ',$user);
    $name=ucwords($name);

    $msg= "<html xmlns=\"http://www.w3.org/1999/xhtml\" style=\"-webkit-box-sizing: border-box;box-sizing: border-box;font-family: sans-serif;line-height: 1.15;-ms-text-size-adjust: 100%;-webkit-text-size-adjust: 100%;-ms-overflow-style: scrollbar;-webkit-tap-highlight-color: transparent;\">\n";
    $msg.= "  <head style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">\n";
    $msg.= "    <meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">\n";
    $msg.= "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">\n";
    $msg.= "    <title style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">$titulo</title>\n";
    $msg.= "    <link rel=\"stylesheet\" href=\"https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-alpha.6/css/bootstrap.min.css\" integrity=\"sha384-rwoIResjU2yc3z8GV/NPeZWAv56rSmLldC3R/AZzGRnGxQQKnKkoFVhFQhNUwEyJ\" crossorigin=\"anonymous\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">\n";
    $msg.= "  </head>\n";
    $msg.= "  <body style=\"-webkit-box-sizing: inherit;box-sizing: inherit;margin: 0;font-family: -apple-system,system-ui,BlinkMacSystemFont,&quot;Segoe UI&quot;,Roboto,&quot;Helvetica Neue&quot;,Arial,sans-serif;font-size: 1rem;font-weight: 400;line-height: 1.5;color: #292b2c;background-color: #fff;\">\n";
    $msg.= "    <div class=\"container\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;margin-left: auto;margin-right: auto;padding-right: 15px;padding-left: 15px;\">\n";
    $msg.= "      <div class=\"card text-center\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-box-orient: vertical;-webkit-box-direction: normal;-webkit-flex-direction: column;-ms-flex-direction: column;flex-direction: column;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-radius: .25rem;text-align: center!important;\">\n";
    $msg.= "        <div class=\"card-header\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;padding: .75rem 1.25rem;margin-bottom: 0;background-color: #f7f7f9;border-bottom: 1px solid rgba(0,0,0,.125);border-radius: calc(.25rem - 1px) calc(.25rem - 1px) 0 0;\">\n";
    $msg.= "          ComeyCome\n";
    $msg.= "        </div>\n";
    $msg.= "        <div class=\"card-block\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;-webkit-box-flex: 1;-webkit-flex: 1 1 auto;-ms-flex: 1 1 auto;flex: 1 1 auto;padding: 1.25rem;\">\n";
    $msg.= "          <h4 class=\"card-title\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;margin-top: 0;margin-bottom: .75rem;font-family: inherit;font-weight: 500;line-height: 1.1;color: inherit;font-size: 1.5rem;\">Solicitud de Baja</h4>\n";
    $msg.= "          <p class=\"card-text\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;orphans: 3;widows: 3;margin-top: 0;margin-bottom: 1rem;\">Hola $name!, <strong class=\"font-weight-bold\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;font-weight: 700;\">".$registerer."</strong> ha registrado una <strong class=\"font-weight-bold\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;font-weight: 700;\">$action</strong> en el ComeyCome. A continuación los detalles</p>\n";
    $msg.= "          <hr class=\"my-4\" style=\"-webkit-box-sizing: content-box;box-sizing: content-box;height: 0;overflow: visible;margin-top: 1.5rem!important;margin-bottom: 1.5rem!important;border: 0;border-top: 1px solid rgba(0,0,0,.1);\">\n";
    $msg.= "          <div class=\"d-flex justify-content-around align-items-center\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;display: flex!important;-webkit-justify-content: space-around!important;-ms-flex-pack: distribute!important;justify-content: space-around!important;-webkit-box-align: center!important;-webkit-align-items: center!important;-ms-flex-align: center!important;align-items: center!important;\">\n";
    $msg.= "\n";
    $msg.= "            <!-- OLD Puesto -->\n";
    $msg.= "            <div class=\"p-2 card\" style=\"width: 80%;-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-box-orient: vertical;-webkit-box-direction: normal;-webkit-flex-direction: column;-ms-flex-direction: column;flex-direction: column;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-radius: .25rem;padding: .5rem .5rem!important;\">\n";
    $msg.= "              <div class=\"card-block\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;-webkit-box-flex: 1;-webkit-flex: 1 1 auto;-ms-flex: 1 1 auto;flex: 1 1 auto;padding: 1.25rem;\">\n";
    $msg.= "                <h4 class=\"card-title\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;margin-top: 0;margin-bottom: .75rem;font-family: inherit;font-weight: 500;line-height: 1.1;color: inherit;font-size: 1.5rem;\">".$m_data['nombreAsesor']."</h4>\n";
    $msg.= "                <blockquote class=\"blockquote blockquote-reverse\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;border: 0px solid #999;page-break-inside: avoid;margin: 0 0 1rem;padding: .5rem 1rem;margin-bottom: 1rem;font-size: 1.25rem;border-left: 0;padding-right: 1rem;padding-left: 0;text-align: right;border-right: .25rem solid #eceeef;\">\n";
    $msg.= "                  <p class=\"mb-0\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;orphans: 3;widows: 3;margin-top: 0;margin-bottom: 0!important;\"><em style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">".$m_data['comentarios']."</em></p>\n";
    $msg.= "                  <footer class=\"blockquote-footer\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;display: block;font-size: 80%;color: #636c72;\">".$m_data['sol']."</footer>\n";
    $msg.= "                </blockquote>\n";

    if($tipo == 'set'){
      $msg.= "                <blockquote class=\"blockquote blockquote-reverse\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;border: 0px solid #999;page-break-inside: avoid;margin: 0 0 1rem;padding: .5rem 1rem;margin-bottom: 1rem;font-size: 1.25rem;border-left: 0;padding-right: 1rem;padding-left: 0;text-align: right;border-right: .25rem solid #eceeef;\">\n";
      $msg.= "                  <p class=\"mb-0\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;orphans: 3;widows: 3;margin-top: 0;margin-bottom: 0!important;\"><em style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">".$m_data['comentariosRRHH']."</em></p>\n";
      $msg.= "                  <footer class=\"blockquote-footer\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;display: block;font-size: 80%;color: #636c72;\">Recursos Humanos</footer>\n";
      $msg.= "                </blockquote>\n";
    }

    $msg.= "              </div>\n";
    $msg.= "              <ul class=\"list-group list-group-flush\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;margin-top: 0;margin-bottom: 0;display: flex;-webkit-box-orient: vertical;-webkit-box-direction: normal;-webkit-flex-direction: column;-ms-flex-direction: column;flex-direction: column;padding-left: 0;\">\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-top-right-radius: .25rem;border-top-left-radius: .25rem;border-right: 0;border-left: 0;border-radius: 0;\">\n";
    $msg.= "\n";
    $msg.= "                    <div class=\"col-6 text-right\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: right!important;\">\n";
    $msg.= "                      UDN / Area / Departamento:\n";
    $msg.= "                    </div>\n";
    $msg.= "                    <div class=\"col-6 text-left\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: left!important;\">\n";
    $msg.= "                      ".$m_data['old']['udn']."<br>".$m_data['old']['area']."<br>".$m_data['old']['dep']."\n";
    $msg.= "                    </div>\n";
    $msg.= "\n";
    $msg.= "                </li>\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-right: 0;border-left: 0;border-radius: 0;\">\n";
    $msg.= "\n";
    $msg.= "                    <div class=\"col-6 text-right\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: right!important;\">\n";
    $msg.= "                      Puesto:\n";
    $msg.= "                    </div>\n";
    $msg.= "                    <div class=\"col-6 text-left\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: left!important;\">\n";
    $msg.= "                      ".$m_data['old']['puestoRH']." (".$m_data['old']['puesto'].")<br>".$m_data['old']['codigo']."\n";
    $msg.= "                    </div>\n";
    $msg.= "\n";
    $msg.= "                </li>\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-right: 0;border-left: 0;border-radius: 0;\">\n";
    $msg.= "\n";
    $msg.= "                    <div class=\"col-6 text-right\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: right!important;\">\n";
    $msg.= "                      Fecha de Baja Solicitada:\n";
    $msg.= "                    </div>\n";
    $msg.= "                    <div class=\"col-6 text-left\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: left!important;\">\n";
    $msg.= "                      ".$m_data['fechaBaja']."\n";
    $msg.= "                    </div>\n";
    $msg.= "\n";
    $msg.= "                </li>\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-right: 0;border-left: 0;border-radius: 0;\">\n";
    $msg.= "\n";
    $msg.= "                    <div class=\"col-6 text-right\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: right!important;\">\n";
    $msg.= "                      Recontratable:\n";
    $msg.= "                    </div>\n";
    $msg.= "                    <div class=\"col-6 text-left\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: left!important;\">\n";

    if($m_data['recontratable']==1){
      $msg.= "                      <span class=\"badge badge-success\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;border: 0px solid #000;display: inline-block;padding: .25em .4em;font-size: 75%;font-weight: 700;line-height: 1;color: #fff;text-align: center;white-space: nowrap;vertical-align: baseline;border-radius: .25rem;background-color: #5cb85c;\">Recontratable</span>\n";
    }else{
      $msg.= "                      <span class=\"badge badge-danger\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;border: 0px solid #000;display: inline-block;padding: .25em .4em;font-size: 75%;font-weight: 700;line-height: 1;color: #fff;text-align: center;white-space: nowrap;vertical-align: baseline;border-radius: .25rem;background-color: #d9534f;\">No Recontratable</span>\n";
    }


    $msg.= "                    </div>\n";
    $msg.= "\n";
    $msg.= "                </li>\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: 0;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-bottom-right-radius: .25rem;border-bottom-left-radius: .25rem;border-right: 0;border-left: 0;border-radius: 0;\">\n";
    $msg.= "\n";
    $msg.= "                    <div class=\"col-6 text-right\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: right!important;\">\n";
    $msg.= "                      Reemplazable:\n";
    $msg.= "                    </div>\n";
    $msg.= "                    <div class=\"col-6 text-left\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: left!important;\">\n";

    if($m_data['reemplazable']==1){
      $msg.= "                      <span class=\"badge badge-success\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;border: 0px solid #000;display: inline-block;padding: .25em .4em;font-size: 75%;font-weight: 700;line-height: 1;color: #fff;text-align: center;white-space: nowrap;vertical-align: baseline;border-radius: .25rem;background-color: #5cb85c;\">Reemplazable ".$m_data['fechaLiberacion']."</span>\n";
    }else{
      $msg.= "                      <span class=\"badge badge-danger\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;border: 0px solid #000;display: inline-block;padding: .25em .4em;font-size: 75%;font-weight: 700;line-height: 1;color: #fff;text-align: center;white-space: nowrap;vertical-align: baseline;border-radius: .25rem;background-color: #d9534f;\">No Reemplazable</span>\n";
    }

    $msg.= "                    </div>\n";
    $msg.= "\n";
    $msg.= "                </li>\n";
    $msg.= "\n";
    $msg.= "              </ul>\n";

    if($tipo == 'ask'){
      $msg.= "              <div class=\"card-block text-center\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;-webkit-box-flex: 1;-webkit-flex: 1 1 auto;-ms-flex: 1 1 auto;flex: 1 1 auto;padding: 1.25rem;text-align: center!important;\">\n";
      $msg.= "                <p class=\"text-center\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;orphans: 3;widows: 3;margin-top: 0;margin-bottom: 1rem;text-align: center!important;\">\n";
      $msg.= "                  <a class=\"btn btn-primary btn-lg\" href=\"#\" role=\"button\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;background-color: #0275d8;-webkit-text-decoration-skip: objects;color: #fff;text-decoration: underline;-ms-touch-action: manipulation;touch-action: manipulation;cursor: pointer;display: inline-block;font-weight: 400;line-height: 1.25;text-align: center;white-space: nowrap;vertical-align: middle;-webkit-user-select: none;-moz-user-select: none;-ms-user-select: none;user-select: none;border: 1px solid transparent;padding: .75rem 1.5rem;font-size: 1.25rem;border-radius: .3rem;-webkit-transition: all .2s ease-in-out;-o-transition: all .2s ease-in-out;transition: all .2s ease-in-out;border-color: #0275d8;\">Ver Solicitud</a>\n";
      $msg.= "                </p>\n";
      $msg.= "              </div>\n";
    }else{
      $msg.= "              <div class=\"card-block text-center\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;-webkit-box-flex: 1;-webkit-flex: 1 1 auto;-ms-flex: 1 1 auto;flex: 1 1 auto;padding: 1.25rem;text-align: center!important;\">\n";
      $msg.= "                <p class=\"text-center\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;orphans: 3;widows: 3;margin-top: 0;margin-bottom: 1rem;text-align: center!important;\">\n";
      if($m_data['status']==1){
          $msg.= "                  <span class=\"btn btn-success btn-lg\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;display: inline-block;font-weight: 400;line-height: 1.25;text-align: center;white-space: nowrap;vertical-align: middle;-webkit-user-select: none;-moz-user-select: none;-ms-user-select: none;user-select: none;border: 1px solid transparent;padding: .75rem 1.5rem;font-size: 1.25rem;border-radius: .3rem;-webkit-transition: all .2s ease-in-out;-o-transition: all .2s ease-in-out;transition: all .2s ease-in-out;color: #fff;background-color: #5cb85c;border-color: #5cb85c;\">Aprobada</span>\n";
      }else{
        $msg.= "                  <span class=\"btn btn-danger btn-lg\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;display: inline-block;font-weight: 400;line-height: 1.25;text-align: center;white-space: nowrap;vertical-align: middle;-webkit-user-select: none;-moz-user-select: none;-ms-user-select: none;user-select: none;border: 1px solid transparent;padding: .75rem 1.5rem;font-size: 1.25rem;border-radius: .3rem;-webkit-transition: all .2s ease-in-out;-o-transition: all .2s ease-in-out;transition: all .2s ease-in-out;color: #fff;background-color: #d9534f;border-color: #d9534f;\">Denegada</span>\n";
      }
      $msg.= "                </p>\n";
      $msg.= "              </div>\n";
    }

    $msg.= "            </div>\n";
    $msg.= "          </div>\n";
    $msg.= "        </div>\n";
    $msg.= "        <div class=\"card-footer text-muted\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;padding: .75rem 1.25rem;background-color: #f7f7f9;border-top: 1px solid rgba(0,0,0,.125);border-radius: 0 0 calc(.25rem - 1px) calc(.25rem - 1px);color: #636c72!important;\">\n";
    $msg.= "          © ComeyCome 2017. Todos los derechos reservados\n";
    $msg.= "        </div>\n";
    $msg.= "      </div>\n";
    $msg.= "\n";
    $msg.= "    </div>\n";
    $msg.= "  </body>\n";
    $msg.= "</html>\n";
    $msg.= "\n";

    $headers = "MIME-Version: 1.0" . "\r\n";
    $headers .= "Content-type:text/html;charset=UTF-8" . "\r\n";
    $headers .= "From: Notificaciones ComeyCome <operaciones@pricetravel.com>";

    mail("$user@pricetravel.com",$titulo,$msg,$headers);

    // echo $msg;

  }


}

class solicitudVacante{

  public static function mail( $class, $params, $tipo ){

    $mailInfo['vacante']          = $params['vacante'];
    $mailInfo['status']           = $params['status'];
    $mailInfo['cantidad']         = $params['cantidad'];
    $mailInfo['applier']          = $params['applier'];

    $query="SELECT
                a.id,
                a.comentarios,
                NOMBREASESOR(a.created_by, 1) AS creador,
                NOMBREASESOR(".$mailInfo['applier'].", 1) AS applier,
                a.inicio,
                b.UDN_nombre AS udn,
                b.Area_nombre AS area,
                b.Departamento_nombre AS departamento,
                b.Puesto_nombre AS puesto,
                d.Puesto AS alias,
                c.PDV AS oficina,
                b.Codigo as codigo
            FROM
                asesores_plazas a
                    LEFT JOIN
                (SELECT
                    a.id AS puestoID,
                        d.clave AS UDN,
                        c.clave AS Area,
                        b.clave AS Departamento,
                        a.clave AS Puesto,
                        CONCAT(d.clave, '-', c.clave, '-', b.clave, '-', a.clave) AS Codigo,
                        d.nombre AS UDN_nombre,
                        c.nombre AS Area_nombre,
                        b.nombre AS Departamento_nombre,
                        a.nombre AS Puesto_nombre
                FROM
                    hc_codigos_Puesto a
                LEFT JOIN hc_codigos_Departamento b ON a.departamento = b.id
                LEFT JOIN hc_codigos_Areas c ON b.area = c.id
                LEFT JOIN hc_codigos_UnidadDeNegocio d ON c.unidadDeNegocio = d.id) b ON a.hc_puesto = b.puestoID
                    LEFT JOIN
                PDVs c ON a.oficina = c.id
                    LEFT JOIN
                PCRCs_puestos d ON a.puesto = d.id
            WHERE
                a.id = ".$mailInfo['vacante'];

    $q = $class->db->query($query);
    $mailInfo['data'] = $q->row_array();

    if($tipo == 'ask'){
      $query="SELECT usuario FROM mail_lists WHERE notif='vacanteSOL'";
      $q = $class->db->query($query);
      foreach ($q->result() as $row){
        solicitudVacante::sendMail($row->usuario,$mailInfo, 'ask');
        $result[] = $row->usuario;
      }
    }else{
      $query="SELECT usuario FROM mail_lists WHERE notif='vacanteOK'";
      $q = $class->db->query($query);
      foreach ($q->result() as $row){
        solicitudVacante::sendMail($row->usuario,$mailInfo, 'set');
        $result[] = $row->usuario;
      }
    }

  }

  public static function sendMail($user, $m_data, $tipo){

    switch($tipo){
      case 'ask':
        $titulo = "Solicitud de Vacante (".$m_data['cantidad'].")";
        $action = "solicitud de vacante";
        break;
      case 'set':
        if($m_data['status']==1){
          $titulo = "Vacante aprobada (".$m_data['data']['id'].")";
          $action = "aprobación de vacante";
        }else{
          $titulo = "Vacante denegada (".$m_data['data']['id'].")";
          $action = "denegación de vacante";
        }
        break;
    }

    $name=str_replace('.',' ',$user);
    $name=ucwords($name);

    $msg= "<html xmlns=\"http://www.w3.org/1999/xhtml\" style=\"-webkit-box-sizing: border-box;box-sizing: border-box;font-family: sans-serif;line-height: 1.15;-ms-text-size-adjust: 100%;-webkit-text-size-adjust: 100%;-ms-overflow-style: scrollbar;-webkit-tap-highlight-color: transparent;\">\n";
    $msg.= "  <head style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">\n";
    $msg.= "    <meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">\n";
    $msg.= "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">\n";
    $msg.= "    <title style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">$titulo</title>\n";
    $msg.= "  </head>\n";
    $msg.= "  <body style=\"-webkit-box-sizing: inherit;box-sizing: inherit;margin: 0;font-family: -apple-system,system-ui,BlinkMacSystemFont,&quot;Segoe UI&quot;,Roboto,&quot;Helvetica Neue&quot;,Arial,sans-serif;font-size: 1rem;font-weight: 400;line-height: 1.5;color: #292b2c;background-color: #fff;\">\n";
    $msg.= "    <div class=\"container\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;margin-left: auto;margin-right: auto;padding-right: 15px;padding-left: 15px; min-width: 900px; max-width:1200px\">\n";
    $msg.= "      <div class=\"card text-center\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-box-orient: vertical;-webkit-box-direction: normal;-webkit-flex-direction: column;-ms-flex-direction: column;flex-direction: column;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-radius: .25rem;text-align: center!important;\">\n";
    $msg.= "        <div class=\"card-header\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;padding: .75rem 1.25rem;margin-bottom: 0;background-color: #f7f7f9;border-bottom: 1px solid rgba(0,0,0,.125);border-radius: calc(.25rem - 1px) calc(.25rem - 1px) 0 0;\">\n";
    $msg.= "          ComeyCome\n";
    $msg.= "        </div>\n";
    $msg.= "        <div class=\"card-block\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;-webkit-box-flex: 1;-webkit-flex: 1 1 auto;-ms-flex: 1 1 auto;flex: 1 1 auto;padding: 1.25rem;\">\n";
    $msg.= "          <h4 class=\"card-title\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;margin-top: 0;margin-bottom: .75rem;font-family: inherit;font-weight: 500;line-height: 1.1;color: inherit;font-size: 1.5rem;\">$titulo\n";
    $msg.= "          </h4>\n";
    $msg.= "          <p class=\"card-text\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;orphans: 3;widows: 3;margin-top: 0;margin-bottom: 1rem;\">Hola\n";
    $msg.= "            <custom title=\"usuario\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">$name</custom>\n";
    $msg.= "          !,<strong class=\"font-weight-bold\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;font-weight: 700;\">\n";
    if($tipo == 'set'){
      $msg.= "            <custom title=\"creador\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">".$m_data['data']['applier']."</custom>\n";
    }else{
      $msg.= "            <custom title=\"creador\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">".$m_data['data']['creador']."</custom>\n";
    }
    $msg.= "          </strong> ha registrado una <strong class=\"font-weight-bold\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;font-weight: 700;\">$action</strong> en el ComeyCome. A continuación los detalles</p>\n";
    $msg.= "          <hr class=\"my-4\" style=\"-webkit-box-sizing: content-box;box-sizing: content-box;height: 0;overflow: visible;margin-top: 1.5rem!important;margin-bottom: 1.5rem!important;border: 0;border-top: 1px solid rgba(0,0,0,.1);\">\n";
    $msg.= "          <div class=\"d-flex justify-content-around align-items-center\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;display: flex!important;-webkit-justify-content: space-around!important;-ms-flex-pack: distribute!important;justify-content: space-around!important;-webkit-box-align: center!important;-webkit-align-items: center!important;-ms-flex-align: center!important;align-items: center!important;\">\n";
    $msg.= "\n";
    $msg.= "            <!-- OLD Puesto -->\n";
    $msg.= "            <div class=\"p-2 card\" style=\"width: 80%;-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-box-orient: vertical;-webkit-box-direction: normal;-webkit-flex-direction: column;-ms-flex-direction: column;flex-direction: column;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-radius: .25rem;padding: .5rem .5rem!important;\">\n";
    $msg.= "              <div class=\"card-block\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;-webkit-box-flex: 1;-webkit-flex: 1 1 auto;-ms-flex: 1 1 auto;flex: 1 1 auto;padding: 1.25rem;\">\n";
    if($tipo == 'set'){
      $msg.= "                <h4 class=\"card-title\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;margin-top: 0;margin-bottom: .75rem;font-family: inherit;font-weight: 500;line-height: 1.1;color: inherit;font-size: 1.5rem;\">Moper: ".$m_data['data']['id']."</h4>\n";
    }
    $msg.= "                <blockquote class=\"blockquote blockquote-reverse\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;border: 0px solid #999;page-break-inside: avoid;margin: 0 0 1rem;padding: .5rem 1rem;margin-bottom: 1rem;font-size: 1.25rem;border-left: 0;padding-right: 1rem;padding-left: 0;text-align: right;border-right: .25rem solid #eceeef;\">\n";
    $msg.= "                  <custom title=\"comentarios\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\"></custom><p class=\"mb-0\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;orphans: 3;widows: 3;margin-top: 0;margin-bottom: 0!important;\"><em style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">".$m_data['data']['comentarios']."</em></p>\n";
    $msg.= "                  <custom title=\"creador\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\"><footer class=\"blockquote-footer\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;display: block;font-size: 80%;color: #636c72;\">".$m_data['data']['creador']."</footer></custom>\n";
    $msg.= "                </blockquote>\n";
    $msg.= "              </div>\n";
    $msg.= "              <ul class=\"list-group list-group-flush\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;margin-top: 0;margin-bottom: 0;display: flex;-webkit-box-orient: vertical;-webkit-box-direction: normal;-webkit-flex-direction: column;-ms-flex-direction: column;flex-direction: column;padding-left: 0;\">\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-top-right-radius: .25rem;border-top-left-radius: .25rem;border-right: 0;border-left: 0;border-radius: 0;\">\n";
    $msg.= "\n";
    $msg.= "                    <div class=\"col-6 text-right\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: right!important;\">\n";
    $msg.= "                      UDN / Area / Departamento:\n";
    $msg.= "                    </div>\n";
    $msg.= "                    <div class=\"col-6 text-left\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: left!important;\">\n";
    $msg.= "                      <custom style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">".$m_data['data']['udn']."<br>".$m_data['data']['area']."<br>".$m_data['data']['departamento']."</custom>\n";
    $msg.= "                    </div>\n";
    $msg.= "\n";
    $msg.= "                </li>\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-right: 0;border-left: 0;border-radius: 0;\">\n";
    $msg.= "\n";
    $msg.= "                    <div class=\"col-6 text-right\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: right!important;\">\n";
    $msg.= "                      Puesto:\n";
    $msg.= "                    </div>\n";
    $msg.= "                    <div class=\"col-6 text-left\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: left!important;\">\n";
    $msg.= "                      <custom style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">".$m_data['data']['puesto']." (".$m_data['data']['alias'].")<br>".$m_data['data']['codigo']."</custom>\n";
    $msg.= "                    </div>\n";
    $msg.= "\n";
    $msg.= "                </li>\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-right: 0;border-left: 0;border-radius: 0;\">\n";
    $msg.= "\n";
    $msg.= "                    <div class=\"col-6 text-right\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: right!important;\">\n";
    $msg.= "                      PDV/Oficina:\n";
    $msg.= "                    </div>\n";
    $msg.= "                    <div class=\"col-6 text-left\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: left!important;\">\n";
    $msg.= "                      <custom style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">".$m_data['data']['oficina']."</custom>\n";
    $msg.= "                    </div>\n";
    $msg.= "\n";
    $msg.= "                </li>\n";
    $msg.= "                <li class=\"list-group-item\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;display: flex;-webkit-flex-flow: row wrap;-ms-flex-flow: row wrap;flex-flow: row wrap;-webkit-box-align: center;-webkit-align-items: center;-ms-flex-align: center;align-items: center;padding: .75rem 1.25rem;margin-bottom: -1px;background-color: #fff;border: 1px solid rgba(0,0,0,.125);border-right: 0;border-left: 0;border-radius: 0;\">\n";
    $msg.= "\n";
    $msg.= "                    <div class=\"col-6 text-right\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: right!important;\">\n";
    $msg.= "                      Fecha de Inicio:\n";
    $msg.= "                    </div>\n";
    $msg.= "                    <div class=\"col-6 text-left\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;position: relative;width: 100%;min-height: 1px;padding-right: 15px;padding-left: 15px;-webkit-box-flex: 0;-webkit-flex: 0 0 50%;-ms-flex: 0 0 50%;flex: 0 0 50%;max-width: 50%;text-align: left!important;\">\n";
    $msg.= "                      <custom style=\"-webkit-box-sizing: inherit;box-sizing: inherit;\">".$m_data['data']['inicio']."</custom>\n";
    $msg.= "                    </div>\n";
    $msg.= "\n";
    $msg.= "                </li>\n";
    $msg.= "                <div class=\"card-block text-center\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;-webkit-box-flex: 1;-webkit-flex: 1 1 auto;-ms-flex: 1 1 auto;flex: 1 1 auto;padding: 1.25rem;text-align: center!important;\">\n";
    $msg.= "                <p class=\"text-center\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;orphans: 3;widows: 3;margin-top: 0;margin-bottom: 1rem;text-align: center!important;\">\n";

    if($tipo == 'set'){
      if($m_data['status'] == 1){
        $msg.= "                  <span class=\"btn btn-success btn-lg\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;display: inline-block;font-weight: 400;line-height: 1.25;text-align: center;white-space: nowrap;vertical-align: middle;-webkit-user-select: none;-moz-user-select: none;-ms-user-select: none;user-select: none;border: 1px solid transparent;padding: .75rem 1.5rem;font-size: 1.25rem;border-radius: .3rem;-webkit-transition: all .2s ease-in-out;-o-transition: all .2s ease-in-out;transition: all .2s ease-in-out;color: #fff;background-color: #5cb85c;border-color: #5cb85c;\">Aprobada</span>\n";
      }else{
        $msg.= "                  <span class=\"btn btn-danger btn-lg\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;display: inline-block;font-weight: 400;line-height: 1.25;text-align: center;white-space: nowrap;vertical-align: middle;-webkit-user-select: none;-moz-user-select: none;-ms-user-select: none;user-select: none;border: 1px solid transparent;padding: .75rem 1.5rem;font-size: 1.25rem;border-radius: .3rem;-webkit-transition: all .2s ease-in-out;-o-transition: all .2s ease-in-out;transition: all .2s ease-in-out;color: #fff;background-color: #d9534f;border-color: #d9534f;\">Denegada</span>\n";
      }
    }else{
      $msg.= "                  <a href=\"https://operaciones.pricetravel.com.mx/config/aprobaciones_pendientes.php\" class=\"btn btn-primary btn-lg\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;background-color: #0275d8;-webkit-text-decoration-skip: objects;color: #fff;text-decoration: underline;-ms-touch-action: manipulation;touch-action: manipulation;display: inline-block;font-weight: 400;line-height: 1.25;text-align: center;white-space: nowrap;vertical-align: middle;-webkit-user-select: none;-moz-user-select: none;-ms-user-select: none;user-select: none;border: 1px solid transparent;padding: .75rem 1.5rem;font-size: 1.25rem;border-radius: .3rem;-webkit-transition: all .2s ease-in-out;-o-transition: all .2s ease-in-out;transition: all .2s ease-in-out;border-color: #0275d8;\">Ver Solicitud</a>\n";
    }

    $msg.= "                </p>\n";
    $msg.= "              </div>\n";
    $msg.= "            </ul></div>\n";
    $msg.= "          </div>\n";
    $msg.= "        </div>\n";
    $msg.= "        <div class=\"card-footer text-muted\" style=\"-webkit-box-sizing: inherit;box-sizing: inherit;padding: .75rem 1.25rem;background-color: #f7f7f9;border-top: 1px solid rgba(0,0,0,.125);border-radius: 0 0 calc(.25rem - 1px) calc(.25rem - 1px);color: #636c72!important;\">\n";
    $msg.= "          © ComeyCome 2017. Todos los derechos reservados\n";
    $msg.= "        </div>\n";
    $msg.= "      </div>\n";
    $msg.= "\n";
    $msg.= "    </div>\n";
    $msg.= "  </body>\n";
    $msg.= "</html>\n";
    $msg.= "\n";

    $headers = "MIME-Version: 1.0" . "\r\n";
    $headers .= "Content-type:text/html;charset=UTF-8" . "\r\n";
    $headers .= "From: Notificaciones ComeyCome <operaciones@pricetravel.com>";

    mail("$user@pricetravel.com",$titulo,$msg,$headers);

  }


}

?>
