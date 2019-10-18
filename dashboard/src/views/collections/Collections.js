import React,{useState} from "react";
import {useGetCollections} from '../../hooks/collections';
import {List,Button,Modal } from 'antd'
import CreateCollectionForm from './create/CreateForm'

import Collection from '../../components/collection/Collection'
import './Collections.css'
import Loading from '../../components/loading/Loading'
import {useCreateCollection} from '../../hooks/collections'

const Collections = () => {
  const [showModal,setShowModal]=useState(false)
  const [collections,isLoadingGet] = useGetCollections([]);
  const [createdCollection, createCollection,isLoadingCreate]=useCreateCollection()
  const isLoading = isLoadingCreate || isLoadingGet
  if (isLoading && !collections) {
      return <Loading />
    }
    return (
    <div>
      <h1>Collections</h1>
      <Button type="primary" icon="plus"
      onClick={()=>{setShowModal(true)}}>
        Create
      </Button>
        {showModal && 
          <CreateCollectionForm 
          show={showModal} 
          onCancel={()=>{setShowModal(false)}} 
          onSubmit={(values)=>{
            console.log(values)
            createCollection({
              name: values.documentName,
              idKey: values.idKey,
              orderingKey: values.orderingKey,
		          active: true
            })
            setShowModal(false)
          }}
          onError={(e)=>console.error(e)}
          />}
        {!isLoading && collections &&  <List
           grid={{
            gutter: 16,
            xs: 1,
            sm: 1,
            md: 1,
            lg: 1,
            xl: 1,
            xxl: 1,
          }}
          dataSource={collections}
          renderItem={item=>document(item)} /> }

  
    </div>
  );
};

const document = (collection)=>{
  return (
              <List.Item key={document.id}><Collection  collection={collection} /></List.Item>
  )
}

export default React.memo(Collections);