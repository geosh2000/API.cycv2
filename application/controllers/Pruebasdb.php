<?php
defined('BASEPATH') OR exit('No direct script access allowed');




class Pruebasdb  extends CI_Controller {

        public function __construct(){

          parent::__construct();
          $this->load->database();
          $this->load->helper('json_utilities');

        }

        public function insert(){

          $values = array(
            'id' => '170',
            'Nombre_Separado' => 'Jorge Alberto',
          );

          // $this->db->set($values);
          // $insertQuery = $this->db->get_compiled_insert('Asesores_test');
          //
          // $onDup = "$insertQuery ON DUPLICATE KEY UPDATE ".onDuplicateUpdateValueSet( $values );

          onDuplicateUpdate($this, $values);

        }

        public function tabla( ){

          $this->db->select('*')
            ->from('Asesores_test')
              ->like('Nombre','Jorge alberto');

          $clientes = $this->db->get();

          prettyPrint( $clientes->result() );

        }

        public function clientes_beta(){

          $query = $this->db->query("SELECT * FROM Asesores LIMIT 10");

          // foreach($query->result() as $row){
          //   echo $row->id;
          //   echo $row->Nombre;
          //   echo $row->Usuario;
          // }
          //
          // echo "Total registros ".$query->num_rows();


          $respuesta = array(
            'err' => FALSE,
            'msg' => "Registros cargados correctamente",
            'total_regs' => $query->num_rows(),
            'clientes' => $query->result()
          );

          prettyPrint( $respuesta );
        }

        public function cliente( $id ){

          $query = $this->db->query("SELECT * FROM Asesores WHERE id=$id");

          $fila= $query->row();

          $respuesta = array(
            'err' => FALSE,
            'msg' => "Registros cargados correctamente",
            'total_regs' => $query->num_rows(),
            'cliente' => $fila
          );

          if( isset( $fila ) ){

            $respuesta['err'] = FALSE;
            $respuesta['msg'] = "Registros cargados correctamente";

          }else{

            $respuesta['err'] = TRUE;
            $respuesta['msg'] = "No existen registros con el id: $id";

          }

          prettyPrint( $respuesta );
        }


}
