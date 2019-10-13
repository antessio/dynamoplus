import React from 'react'

import './Indexes.css'
import Loading from '../../components/loading/Loading'
import {List } from 'antd'
import {useGetIndexes} from '../../hooks/indexes'
import Index from '../../components/indexes/Index'

const Indexes = (props)=>{
    const documentType = props.match.params.documentType
    const [indexes,isLoading] = useGetIndexes([],documentType);
    if (isLoading && !indexes) {
        return <Loading />
      }
    return(<div>
        <h2>Indexes</h2>
        <p>{documentType}</p>
        {!isLoading && indexes &&  <List
          dataSource={indexes}
          renderItem={item=>renderIndex(item)} /> }
    </div>)

}
const renderIndex = (index)=>{
    return (
                <List.Item key={index.id}><Index  index={index} /></List.Item>
    )
  }
export default Indexes;