import gql from 'graphql-tag'

const GetGallery = gql`
  query getGallery($input: GetGalleryInput){
    getGallery(input: $input){
      gallery{
        id
        uuid
        listing {
          id
          beds
          baths
          price
          sqft
          type
          mlsStatus
          mlsNumber
          description
          features
        }
        agent {
          id
          avatar {
            id
            url
          }
          type
          name
          email
          contacts {
            id
            type
            content
            default
          }
        }
        address {
          address1
          address2
          city
          state
          zipCode
          lat
          lng
        }
        galleryAssets {
          id
          assetableId
          assetableName
          name
          type
          url
          awsId
          active
          default
          createdAt
        }
      }
    }
  }
`

export default GetGallery