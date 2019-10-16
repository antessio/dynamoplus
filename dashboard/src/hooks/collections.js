import {useState, useEffect} from 'react'

import { useAuth0 } from '../react-auth0-wrapper'



export const useCreateCollection = ()=>{
    const { getTokenSilently } = useAuth0();
    const [collection, setCollection] = useState(null)
    const [isLoading, setLoading] = useState(false)
    const createCollection = async (d) => {
      setLoading(false)
      try {
        const token = await getTokenSilently()
        const response = await fetch(process.env.REACT_APP_API_BASE_PATH+"/dynamoplus/document_type", {
          headers: {
            Authorization: `Bearer ${token}`
          },
          method: 'POST',
          body: JSON.stringify(d)
  
        });
  
        const responseData = await response.json();
        console.log(responseData)
        setCollection(responseData)
        setLoading(false)
      } catch (error) {
        setLoading(false)
      }
    }; 
    return [collection, createCollection, isLoading]
}

export const useGetCollections = (dependencies)=>{
    const { getTokenSilently } = useAuth0();
    const [collections, setCollections] = useState([])
    const [isLoading, setLoading] = useState(false)
    const getCollections = async () => {
        setLoading(true)
        try {
          const token = await getTokenSilently()
          const response = await fetch(process.env.REACT_APP_API_BASE_PATH+"/dynamoplus/document_type/query/active", {
            headers: {
              Authorization: `Bearer ${token}`
            },
            method: 'POST',
            body: JSON.stringify({active: "true"})
    
          });
    
          const responseData = await response.json();
          
          const collections = responseData.data
          console.log(collections)
          setCollections(collections)
          setLoading(false)
        } catch (error) {
          setLoading(false)
        }
      };
    useEffect(() => {
        //setLoading(true)
        getCollections()
      }, dependencies);
    return [collections,isLoading]


}