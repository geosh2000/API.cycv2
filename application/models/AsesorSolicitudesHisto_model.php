<?php
defined('BASEPATH') OR exit('No direct script access allowed');


class AsesorSolicitudesHisto_model extends CI_Model{

  public $id;
  public $statusId;
  public $fechaRequest;
  public $rrhhcomment;
  public $solicitante;
  public $solicitanteID;
  public $fechaSolicitada;
  public $dep;
  public $puesto;
  public $pdv;
  public $ciudad;
  public $solicitudComment;
  public $status;
  public $recontratable;
  public $reemplazable;
  public $tipo;

  public function get_solicitudes( $id, $returnAllNull = FALSE ){

    $this->db->select("a.id,
                      fecha_solicitud as fechaRequest,
                      NOMBREASESOR(solicitado_por, 1) as solicitante,
                      solicitado_por as solicitanteID,
                      recontratable,
                      reemplazable,
                      fecha as fechaSolicitada,
                      CASE
                        WHEN a.tipo = 1 THEN 'Cambio'
                        ELSE 'Baja'
                      END as tipo,
                      c.Departamento as dep,
                      d.Puesto as puesto,
                      e.Ciudad as ciudad,
                      PDV AS pdv,
                      a.comentarios as solicitudComment,
                      a.comentariosRRHH as rrhhcomment,
                      CASE
                          WHEN a.status=0 THEN 'En espera'
                          WHEN a.status=1 THEN 'Aprobada'
                          WHEN a.status=2 THEN 'En proceso de revision'
                          WHEN a.status=3 THEN 'Declinada'
                          WHEN a.status=4 THEN 'Cancelada'
                      END as statusOK,
                      a.status as statusID,
                      CASE
                        WHEN a.status = 0 THEN 'En Espera'
                        WHEN a.status = 1 THEN 'Aprobada'
                        WHEN a.status = 2 THEN 'En Espera de Revision'
                        WHEN a.status = 3 THEN 'Declinada'
                        WHEN a.status = 4 THEN 'Cancelada'
                      END as status", FALSE)
              ->from("rrhh_solicitudesCambioBaja a")
              ->join("asesores_plazas b", 'a.vacante      = b.id'       , 'LEFT')
              ->join("PCRCs c"          , 'b.departamento = c.id'       , 'LEFT')
              ->join("PCRCs_puestos d"  , 'b.puesto       = d.id'       , 'LEFT')
              ->join("db_municipios e"  , 'b.ciudad       = e.id'       , 'LEFT')
              ->join("PDVs f"           , 'b.oficina      = f.id'       , 'LEFT')
              ->where( array('asesor' => $id ) )
              ->order_by( 'fecha_solicitud', 'ASC' );

    $query = $this->db->get();

    $result = $query->custom_result_object('AsesorSolicitudesHisto_model');

    if( !isset($result) && $returnAllNull ){
      return get_object_vars($this);
    }else{
      return $result;
    }

  }

  public function get_SolPendientes( $id ){

    $this->db->select("id")
              ->from("rrhh_solicitudesCambioBaja")
              ->where( array('asesor' => $id ) )
              ->where_in( 'status', array( 0, 2 ) );

    $query = $this->db->get();

    if( $query->num_rows() > 0 ){
      return true;
    }else{
      return false;
    }

  }





}
