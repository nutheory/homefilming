// @flow
import React, { Component } from 'react'
import { css } from 'aphrodite'
import { graphql } from 'react-apollo'
import { find, propEq, reject } from 'ramda'
import jwtDecode from 'jwt-decode'
import Avatar from '../assets/avatar'
import Moment from 'moment'
import GetNotes from '../../queries/note_collections'
import DestroyNote from '../../mutations/destroy_note'
import note from './styles/notes'

type Props = {
  destroyNote: Function,
  model: string,
  modelId: number,
  note: Object
}

class NoteItem extends Component<Props, void> {

  handleRemoveNote: Function

  constructor(){
    super()

    this.handleRemoveNote = this.handleRemoveNote.bind(this)
  }

  async handleRemoveNote(){
    await this.props.destroyNote({
      id: this.props.note.id,
      authorizedId: this.props.note.author.id
    })
  }

  render(){
    const user = jwtDecode(localStorage.getItem('hf_auth_header_token'))
    console.log(this.props.note)
    return(
      <article className={`media box ${css(note.noteItem)}`}>
        <figure className="media-left">
          <Avatar
            size="medium"
            src={ this.props.note.author.avatar ? this.props.note.author.avatar.url : null }
          />
        </figure>
        <div className="media-content">
          <div className="content">
            <p>
              <strong>{ this.props.note.author.name }</strong>
              <small>  { Moment(Date.parse(this.props.note.createdAt)).format('MMM Do YYYY, h:mma') }</small>
              <br />
              { this.props.note.body }
            </p>
          </div>
        </div>
        { this.props.note.author.id == user.id ?
          <div className="media-right">
            <button className="delete" onClick={ this.handleRemoveNote }></button>
          </div>
        : null }
      </article>
    )
  }
}

export default graphql(DestroyNote, {
  props: ({ ownProps, mutate }) => ({
    destroyNote: (props) => mutate({
      variables: { input: props },
      refetchQueries: [{
        query: GetNotes,
        variables: { input: {
          model: ownProps.model,
          modelId: ownProps.modelId } } }] }) })
})(NoteItem)
