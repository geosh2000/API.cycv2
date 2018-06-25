<?php
defined('BASEPATH') OR exit('No direct script access allowed');


class AsesorGeneral_model extends CI_Model{

  public $id;
  public $Nombre;
  public $NCorto;
  public $num_colaborador;
  public $Telefono1;
  public $Telefono2;
  public $Usuario;
  public $correo;
  public $Departamento;
  public $hc_puesto;
  public $oficina;
  public $PDV;
  public $Puesto;
  public $Alias;
  public $Ingreso;
  public $Egreso;
  public $Status;
  public $RFC;
  public $Fecha_Nacimiento;
  public $Nombre_Separado;
  public $Apellidos_Separado;
  public $Vigencia_Pasaporte;
  public $Vigencia_Visa;
  public $profile;
  public $profileID;
  public $Codigo;

  public function get_asesor( $id, $returnAllNull = FALSE ){

    $this->db->select("a.id AS puestoID,
                        d.clave AS UDN,
                        c.clave AS Area,
                        b.clave AS Departamento,
                        a.clave AS Puesto,
                        CONCAT(d.clave, '-', c.clave, '-', b.clave, '-', a.clave) AS Codigo,
                        d.nombre AS UDN_nombre,
                        c.nombre AS Area_nombre,
                        b.nombre AS Departamento_nombre,
                        a.nombre AS Puesto_nombre", FALSE)
            ->from("hc_codigos_Puesto a")
            ->join("hc_codigos_Departamento b"    ,"a.departamento    = b.id", "LEFT")
            ->join("hc_codigos_Areas c"           ,"b.area            = c.id", "LEFT")
            ->join("hc_codigos_UnidadDeNegocio d" ,"c.unidadDeNegocio = d.id", "LEFT");
    $tmp_Codes = $this->db->get_compiled_select();
    $this->db->query("DROP TEMPORARY TABLE IF EXISTS hc_Codes");
    $this->db->query("CREATE TEMPORARY TABLE hc_Codes $tmp_Codes");

    $this->db->select( "a.id,
                        a.Nombre,
                        `N Corto` as NCorto,
                        num_colaborador,
                        Telefono1,
                        Telefono2,
                        Usuario,
                        correo_personal as correo,
                        c.Departamento,
                        b.hc_puesto,
                        e.oficina,
                        f.PDV,
                        g.nombre as Puesto,
                        d.Puesto as Alias,
                        Ingreso,
                        Egreso,
                        IF(Egreso>CURDATE(),'Activo','Inactivo') as Status,
                        RFC,
                        Fecha_Nacimiento,
                        Nombre_Separado,
                        Apellidos_Separado,
                        Vigencia_Pasaporte,
                        Vigencia_Visa,
                        profile_name as profile,
                        i.id as profileID,
                        j.Codigo as Codigo", FALSE )
              ->from("Asesores a")
              ->join("dep_asesores b"     , 'a.id         = b.asesor'   , 'LEFT')
              ->join("PCRCs c"            , 'b.dep        = c.id'       , 'LEFT')
              ->join("PCRCs_puestos d"    , 'b.puesto     = d.id'       , 'LEFT')
              ->join("asesores_plazas e"  , 'b.vacante    = e.id'       , 'LEFT')
              ->join("PDVs f"             , 'e.oficina    = f.id'       , 'LEFT')
              ->join("hc_codigos_Puesto g", 'b.hc_puesto  = g.id'       , 'LEFT')
              ->join("userDB h"           , 'a.id         = h.asesor_id', 'LEFT')
              ->join("profilesDB i"       , 'h.profile    = i.id'       , 'LEFT')
              ->join("hc_Codes j"         , 'b.hc_puesto  = j.puestoID' , 'LEFT')
              ->where( array('a.id' => $id ) );

    $query = $this->db->get();

    $row = $query->custom_row_object(0, 'AsesorGeneral_model');

    if( isset($row) ){
      $row->id              = intval($row->id);
      $row->num_colaborador = intval($row->num_colaborador);
      $row->oficina         = intval($row->oficina);
    }

    if( !isset($row) && $returnAllNull ){
      return get_object_vars($this);
    }else{
      return $row;
    }

  }





}
