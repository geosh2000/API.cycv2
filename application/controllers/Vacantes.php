<?php
defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Vacantes extends REST_Controller {
    
    public function __construct(){

        parent::__construct();
        $this->load->helper('json_utilities');
        $this->load->database();
    }   
    
    public function index_get(){
                            
        $udns = $this->db->select("a.id,
                                        d.nombre as Unidad_de_Negocio,
                                        c.nombre as Area,
                                        b.nombre as Departamento,
                                        a.nombre as Puesto,
                                        CONCAT(d.clave,'-',c.clave,'-',b.clave,'-',a.clave) as Codigo")
                                    ->from("hc_codigos_Puesto a")
                                    ->join("hc_codigos_Departamento b", "a.departamento = b.id", "LEFT")
                                    ->join("hc_codigos_Areas c", "b.area = c.id", "LEFT")
                                    ->join("hc_codigos_UnidadDeNegocio d", "c.unidadDeNegocio = d.id", "LEFT")
                                    ->where("c.id", 8)
                                    ->get_compiled_select();
        $vacantes = $this->db->select("vacante, MAX(fecha_out) AS libre")
                                    ->from("asesores_movimiento_vacantes")
                                    ->group_by("vacante")
                                    ->get_compiled_select();
        
        $first = $this->db->select("a.id,
                            libre,
                            f.id as id_PuestoCode,
                            f.Unidad_de_Negocio,
                            f.Area,
                            f.Departamento,
                            f.Puesto,
                            f.Codigo,
                            b.Departamento AS Departamento_Alias,
                            c.Puesto AS puesto_Alias,
                            d.Ciudad AS ciudad,
                            e.PDV AS oficina,
                            a.inicio,
                            a.fin,
                            a.comentarios,
                            NOMBREASESOR(GETVACANTE(a.id, CURDATE()), 2) AS Asesor_Actual,
                            if( NOMBREASESOR(GETVACANTE(a.id, CURDATE()), 2) IS NULL, if( a.Activo=0 AND a.Status!=0, '2Inactivas', '0Vacantes') ,'1Cubiertas') as 
                            type,
                            GETVACANTE(a.id, CURDATE()) as asesorID,
                            a.Status,
                            a.Activo,
                            a.esquema,
                            a.departamento AS dep_id,
                            a.puesto AS puesto_id,
                            a.oficina AS oficina_id,
                            a.ciudad AS ciudad_id,
                            NOMBREASESOR(approbed_by, 1) AS Aprobada_por,
                            date_approbed AS Fecha_Aprobacion")
                    ->from("asesores_plazas a")
                    ->join("PCRCs b","ON a.departamento = b.id","LEFT")
                    ->join("PCRCs_puestos c","a.puesto = c.id","LEFT")
                    ->join("db_municipios d","a.ciudad = d.id","LEFT")
                    ->join("PDVs e","a.oficina = e.id","LEFT")
                    ->join("($udns) f", "hc_puesto=f.id", "LEFT")
                    ->join("($vacantes) g", "a.id = g.vacante", "LEFT")                                    
                    ->having("f.Unidad_de_Negocio IS NOT NULL")
                    ->order_by("Unidad_de_Negocio, Area, Departamento, Puesto, type, f.id, d.Ciudad, e.PDV, Asesor_Actual, a.id","ASC")
                    ->get();
        
        jsonPrint( $first->result_array() );
//        prettyPrint($first);
    }
    
}