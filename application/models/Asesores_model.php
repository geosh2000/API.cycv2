<?php
defined('BASEPATH') OR exit('No direct script access allowed');


class Asesores_model extends CI_Model{

  public $id;
  public $num_colaborador;
  public $Nombre;
  public $Nombre_Separado;
  public $Apellidos_Separado;
  public $Activo;
  public $Ingreso;
  public $Egreso;
  public $Usuario;
  public $Fecha_Nacimiento;
  public $RFC;
  public $Telefono1;
  public $Telefono2;
  public $correo_personal;
  public $Vigencia_Pasaporte;
  public $Vigencia_Visa;

  public function __construct() {
        $this->{'N Corto'};
    }

  public function get_asesor( $id ){

    $this->db->where( array('id' => $id) );
    $query = $this->db->get('Asesores');

    $row = $query->custom_row_object(0, 'Asesores_model');

    if( isset($row) ){
      $row->id              = intval($row->id);
      $row->num_colaborador = intval($row->num_colaborador);
      $row->Activo          = boolval($row->Activo);
    }

    return $row;

  }





}
