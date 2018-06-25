<?php
defined('BASEPATH') OR exit('No direct script access allowed');


class AsesorVacantesHisto_model extends CI_Model{

  public $id;
  public $fecha;
  public $puesto;
  public $ciudad;
  public $pdv;

  public function get_movimientos( $id, $returnAllNull = FALSE ){

    $this->db->select("a.id,
                      fecha_in as fecha,
                      CONCAT(c.Departamento, ' -> ', d.Puesto) as puesto,
                      e.Ciudad as ciudad,
                      f.PDV as pdv")
              ->from("asesores_movimiento_vacantes a")
              ->join("asesores_plazas b", 'a.vacante      = b.id'       , 'LEFT')
              ->join("PCRCs c"          , 'b.departamento = c.id'       , 'LEFT')
              ->join("PCRCs_puestos d"  , 'b.puesto       = d.id'       , 'LEFT')
              ->join("db_municipios e"  , 'b.ciudad       = e.id'       , 'LEFT')
              ->join("PDVs f"           , 'b.oficina      = f.id'       , 'LEFT')
              ->where( array('asesor_in' => $id ) )
              ->order_by( 'Fecha_in', 'ASC' );

    $query = $this->db->get();

    $result = $query->custom_result_object('AsesorVacantesHisto_model');

    if( !isset($result) && $returnAllNull ){
      return get_object_vars($this);
    }else{
      return $result;
    }

  }





}
