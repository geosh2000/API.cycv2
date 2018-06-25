<?php

defined('BASEPATH') OR exit('No direct script access allowed');

class Cliente_model extends CI_Model{

public $id;
public $newid;
public $num_colaborador;
public $Nombre;
public $Nombre_Separado;
public $Apellidos_Separado;
public $NCoro;
public $idDepartamento;
public $puesto;
public $Activo;
public $on_training;
public $Ingreso;
public $Egreso;
public $Usuario;
public $Esquema;
public $Fecha_Nacimiento;
public $RFC;
public $Telefono1;
public $Telefono2;
public $correo_personal;
public $Vigencia_Pasaporte;
public $Vigencia_Visa;
public $ciudad;
public $pdv;
public $plaz;

  public function get_cliente( $id ){

    $this->id = intval( $id );
    $this->Nombre = "Jorge Alberto Sánchez Hernández";
    $this->Usuario = "albert.sanchez";

    return $this;

  }

  public function insert(){

    return "insertado";

  }

  public function update(){

    return "update";

  }

  public function delete(){

    return "delete";

  }



}
