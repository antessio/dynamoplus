import React, {useState} from 'react'
import { Form, Input, Icon, Button,Modal } from 'antd';

const CreateDocumentForm = (props)=>{
    const [showModal,setShowModal]=useState(props.show)
    //const [docmentDefinition, setDocumentDefinition]=useState([])
    
    const handleSubmit=(e)=>{
        e.preventDefault();
        props.form.validateFields((err, values) => {
            if (!err) {
              props.onSubmit(values)
            }else{
                props.onError(err)
            }
          });
    }
    const handleCancel=()=>{
        props.onCancel()
        setShowModal(false);
    }
    return (
        <Form onSubmit={handleSubmit}>
        <Modal
          visible={showModal}
          title="Create new document"
          onOk={handleSubmit}
          onCancel={handleCancel}
          footer={[
            <Button key="back" onClick={handleCancel}>
              Cancel
            </Button>,
            <Button key="submit" type="primary"  onClick={handleSubmit}>
              Submit
            </Button>]}>

          
            <Form.Item label="Document Name">
          {props.form.getFieldDecorator('documentName', {
                rules: [
                  {
                    required: true,
                    message: 'Please input the document name',
                  },
                ],
              })(<Input />)}
            </Form.Item>
            <Form.Item label="ID key">
              {props.form.getFieldDecorator('idKey', {
                rules: [
                  {
                    required: true,
                    message: 'Please input key identifier',
                  },
                ],
              })(<Input />)}
              </Form.Item>
              <Form.Item label="Sort key">
              {props.form.getFieldDecorator('orderingKey', {
                rules: [
                  {
                    required: true,
                    message: 'Please input sort key',
                  },
                ],
              })(<Input />)}
        </Form.Item>
        </Modal>
        </Form>
        
    )
}
export default Form.create({ name: 'create_document' })(CreateDocumentForm);
