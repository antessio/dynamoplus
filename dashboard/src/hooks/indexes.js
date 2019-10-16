import {useState, useEffect} from 'react'

import { useAuth0 } from '../react-auth0-wrapper'

export const useCreateIndex = ()=>{
    const { getTokenSilently } = useAuth0();
    const [index, setIndex] = useState(null)
    const [isLoading, setLoading] = useState(false)
    const createIndex = async (i) => {
      setLoading(false)
      try {
        const token = await getTokenSilently()
        const response = await fetch(process.env.REACT_APP_API_BASE_PATH+"/dynamoplus/index", {
          headers: {
            Authorization: `Bearer ${token}`
          },
          method: 'POST',
          body: JSON.stringify(i)
  
        });
  
        const responseData = await response.json();
        console.log(responseData)
        setIndex(responseData)
        setLoading(false)
      } catch (error) {
        setLoading(false)
      }
    }; 
    return [index, createIndex, isLoading]
}

export const useGetIndexes = (dependencies, collection)=>{
    const { getTokenSilently } = useAuth0();
    const [indexes, setIndexes] = useState([])
    const [isLoading, setLoading] = useState(false)
    const getIndexes = async (collection) => {
        setLoading(false)
        try {
          const token = await getTokenSilently()
          const response = await fetch(process.env.REACT_APP_API_BASE_PATH+"/dynamoplus/index/query/collection.name", {
            headers: {
              Authorization: `Bearer ${token}`
            },
            method: 'POST',
            body: JSON.stringify(
              {
                collection:{
                  name: collection
                }
              }
            )
    
          });
    
          const responseData = await response.json();
          
          const indexes = responseData.data
          console.log(indexes)
          setIndexes(indexes)
          setLoading(false)
        } catch (error) {
          setLoading(false)
        }
      };
    useEffect(() => {
        //setLoading(true)
        getIndexes(collection)
      }, dependencies);
    return [indexes,isLoading]


}