<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class SolicitudesRH extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('jwt');
    $this->load->helper('mailing');
    $this->load->database();
    $this->load->model('Cliente_model');
  }

  public function getSolicitudesVacantes_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS relPuestos");
      $this->db->query("CREATE TEMPORARY TABLE relPuestos SELECT
                            a.id, b.*, c.PDV, d.Ciudad, NOMBREASESOR(created_by,1) as Creador, a.date_created as FechaSolicitud, a.comentarios
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
                            db_municipios d ON a.ciudad = d.id
                            WHERE
                              a.status=0 AND a.Activo=1");
      $query = "SELECT * FROM relPuestos";

      if($q = $this->db->query($query)){

        $solicitudes = $q->result_array();

        $result       = array(
                              'status'    => true,
                              'rows'      => $q->num_rows(),
                              'data'      => $solicitudes,
                              'msg'       => "Solicitudes Cargadas"
                            );
      }else{
        $result       = array(
                              'status'    => false,
                              'rows'      => 0,
                              'data'      => null,
                              'msg'       => $this->db->error()
                            );
      }

      return $result;

    });

    jsonPrint( $result );

  }

  public function getSolicitudes_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS relPuestos");
      $this->db->query("DROP TEMPORARY TABLE IF EXISTS relPuestosB");
      $this->db->query("CREATE TEMPORARY TABLE relPuestos SELECT
                            a.id, b.*, c.PDV, d.Ciudad
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
                            db_municipios d ON a.ciudad = d.id");
      $this->db->query("CREATE TEMPORARY TABLE relPuestosB SELECT * FROM relPuestos");

      $query = "SELECT
                    a.*,
                    NOMBREASESOR(a.asesor,1) as NombreAsesor,
                    NOMBREASESOR(a.solicitado_por,1) as NombreSolicitante,
                    c.UDN_nombre AS UDN,
                    c.Area_nombre AS Area,
                    c.Departamento_nombre AS Departamento,
                    c.Puesto_nombre AS Puesto,
                    c.PDV as Oficina,
                    c.Ciudad as Ciudad,
                    d.UDN_nombre AS New_UDN,
                    d.Area_nombre AS New_Area,
                    d.Departamento_nombre AS New_Departamento,
                    d.Puesto_nombre AS New_Puesto,
                    d.PDV as New_Oficina,
                    d.Ciudad as New_Ciudad
                FROM
                    rrhh_solicitudesCambioBaja a
                        LEFT JOIN
                    dep_asesores b ON a.asesor = b.asesor
                        AND CURDATE() = b.Fecha
                        LEFT JOIN
                    relPuestos c ON b.vacante=c.id
                      LEFT JOIN
                    relPuestosB d ON a.vacante=d.id
                WHERE
                    a.status = 0";

      if($q = $this->db->query($query)){

        $solicitudes = $q->result_array();

        $result       = array(
                              'status'    => true,
                              'rows'      => $q->num_rows(),
                              'data'      => $solicitudes,
                              'msg'       => "Solicitudes Cargadas"
                            );
      }else{
        $result       = array(
                              'status'    => false,
                              'rows'      => 0,
                              'data'      => null,
                              'msg'       => $this->db->error()
                            );
      }

      return $result;

    });

    jsonPrint( $result );

  }

  public function statusCambio_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $id = $this->uri->segment(3);

      $flag=true;
      $error="";

      $q = $this->db->get_where('rrhh_solicitudesCambioBaja', 'id = '.$id);
      $solicitud = $q->row_array();

      $q = $this->db->get_where('asesores_movimiento_vacantes', 'id = '.$solicitud['movimientoID']);
      $movimiento = $q->row_array();

      if($movimiento['asesor_in'] != NULL){
        $flag=false;
        $error = "La vacante ya fue cubierta";
      }

      $query = "SELECT IF( CURDATE() <= fin,1,0) as status FROM asesores_plazas WHERE id = ".$movimiento['vacante'];
      $q = $this->db->query($query);
      $status = $q->row_array();

      if($status['status'] == 0){
        $flag=false;
        $error = "La vacante ya no se encuentra activa";
      }

      $result       = array(
                              'status'    => $flag,
                              'msg'       => $error
                            );


      return $result;

    });

    jsonPrint( $result );

  }

  public function approbeVacante_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      if($data['accion']){
        $status = 1;
      }else{
        $status = 3;
      }

      $update   =   array(
                          'id'          => $data['solicitud'],
                          'approbed_by' => $data['applier'],
                          'status'      => $status
                        );

      $this->db->set($update)
                ->set('date_approbed', 'NOW()', FALSE)
                ->where("id = ".$data['solicitud']);

      if($this->db->update('asesores_plazas')){
        $result = array(
                        'status'  => true,
                        'msg'     => "Cambio de status completado"
                      );
      }else{
        $result = array(
                        'status'  => true,
                        'msg'     => $this->db->error()
                      );
      }

      return $result;


    });

    jsonPrint( $result );

  }

}
