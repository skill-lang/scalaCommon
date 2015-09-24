package de.ust.skill.common.scala.internal

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import de.ust.skill.common.jvm.streams.FileInputStream
import de.ust.skill.common.scala.api.SkillObject
import de.ust.skill.common.scala.api.WriteMode
import de.ust.skill.common.scala.internal.fieldTypes.AnnotationType
import de.ust.skill.common.scala.api.SkillException
import java.nio.file.Path
import de.ust.skill.common.scala.api.ParseException
import de.ust.skill.common.scala.api.FieldDeclaration
import de.ust.skill.common.scala.api.ParseException

/**
 * @author Timm Felden
 */
trait SkillFileParser[SF <: SkillState] {
  def newPool(
    typeId : Int,
    name : String,
    superPool : StoragePool[_ <: SkillObject, _ <: SkillObject]) : StoragePool[_ <: SkillObject, _ <: SkillObject]

  def makeState(path : Path,
                mode : WriteMode,
                String : StringPool,
                Annotation : AnnotationType,
                types : ArrayBuffer[StoragePool[_ <: SkillObject, _ <: SkillObject]],
                typesByName : HashMap[String, StoragePool[_ <: SkillObject, _ <: SkillObject]]) : SF

  /**
   * read a state from file
   *
   * @note the type parameters are a bold lie that keeps the type checker happy
   */
  def read[T <: B, B <: SkillObject](in : FileInputStream, mode : WriteMode) : SF = {
    // ERROR DETECTION
    var blockCounter = 0;
    var seenTypes = HashSet[String]();
    // this barrier is strictly increasing inside of each block and reset to 0 at the beginning of each block
    var blockIDBarrier : Int = 0;

    // PARSE STATE
    val String = new StringPool(in)
    val types = ArrayBuffer[StoragePool[_ <: SkillObject, _ <: SkillObject]]();
    val Annotation = new AnnotationType(types)
    val typesByName = HashMap[String, StoragePool[_ <: SkillObject, _ <: SkillObject]]();

    // process stream
    while (!in.eof) {
      // string block
      try {
        val count = in.v64.toInt;

        if (0 != count) {
          val offsets = new Array[Int](count);
          for (i ← 0 until count) {
            offsets(i) = in.i32;
          }
          String.stringPositions.sizeHint(String.stringPositions.size + count)
          var last = 0
          for (i ← 0 until count) {
            String.stringPositions.append((in.position + last, offsets(i) - last))
            String.idMap += null
            last = offsets(i)
          }
          in.jump(in.position + last);
        }

      } catch {
        case e : Exception ⇒
          throw ParseException(in, blockCounter, e, "corrupted string block")
      }

      // type block
      try {
        val offset = 0L
        val typeCount = in.v64.toInt

        //
        //         function Parse_Field_Type return Skill.Field_Types.Field_Type is
        //            ID : Natural := Natural (Input.V64);
        //
        //            function Convert is new Ada.Unchecked_Conversion
        //              (Source => Types.Pools.Pool,
        //               Target => Field_Types.Field_Type);
        //         begin
        //            case Id is
        //            when 0 =>
        //               return new Field_Types.Builtin.Constant_I8.Field_Type'
        //                 (Value => Input.I8);
        //            when 1 =>
        //               return new Field_Types.Builtin.Constant_I16.Field_Type'
        //                 (Value => Input.I16);
        //            when 2 =>
        //               return new Field_Types.Builtin.Constant_I32.Field_Type'
        //                 (Value => Input.I32);
        //            when 3 =>
        //               return new Field_Types.Builtin.Constant_I64.Field_Type'
        //                 (Value => Input.I64);
        //            when 4 =>
        //               return new Field_Types.Builtin.Constant_V64.Field_Type'
        //                 (Value => Input.V64);
        //            when 5 =>
        //               return Field_Types.Field_Type(Annotation_Type);
        //            when 6 =>
        //               return Field_Types.Builtin.Bool;
        //            when 7 =>
        //               return Field_Types.Builtin.I8;
        //            when 8 =>
        //               return Field_Types.Builtin.I16;
        //            when 9 =>
        //               return Field_Types.Builtin.I32;
        //            when 10 =>
        //               return Field_Types.Builtin.I64;
        //            when 11 =>
        //               return Field_Types.Builtin.V64;
        //            when 12 =>
        //               return Field_Types.Builtin.F32;
        //            when 13 =>
        //               return Field_Types.Builtin.F64;
        //            when 14 =>
        //               return Field_Types.Field_Type(String_Type);
        //            when 15 =>
        //               declare
        //                  Length : Types.V64 := Input.V64;
        //                  T : Field_Types.Field_Type := Parse_Field_Type;
        //               begin
        //                  return Field_Types.Builtin.Const_Array(Length, T);
        //               end;
        //            when 17 =>
        //               return Field_Types.Builtin.Var_Array (Parse_Field_Type);
        //            when 18 =>
        //               return Skill.Field_Types.Builtin.List_Type(Parse_Field_Type);
        //            when 19 =>
        //               return Skill.Field_Types.Builtin.Set_Type (Parse_Field_Type);
        //            when 20 =>
        //               declare
        //                  K : Field_Types.Field_Type := Parse_Field_Type;
        //                  V : Field_Types.Field_Type := Parse_Field_Type;
        //               begin
        //                  return Skill.Field_Types.Builtin.Map_Type (K, V);
        //               end;
        //            when others =>
        //               if ID >= 32 and Id < Natural(Type_Vector.Length) + 32 then
        //                  return Convert (Type_Vector.Element (ID - 32));
        //               end if;
        //
        //               raise Errors.Skill_Error
        //               with Input.Parse_Exception
        //                 (Block_Counter,
        //                  "Invalid type ID: " & Natural'Image (ID) & " largest is: "
        //                  & Natural'Image (Natural(Type_Vector.Length) + 32));
        //            end case;
        //         end Parse_Field_Type;
        //
        //         procedure Parse_Fields (E : LF_Entry) is
        //            Legal_Field_ID_Barrier : Positive :=
        //                                       1 + Natural(E.Pool.Data_Fields.Length);
        //            Last_Block             : Skill.Internal.Parts.Block :=
        //                                       E.Pool.Blocks.Last_Element;
        //            End_Offset             : Types.V64;
        //         begin
        //            for Field_Counter in 1 .. E.Count loop
        //               declare
        //                  Id : Integer := Integer (Input.V64);
        //               begin
        //
        //                  if Id <= 0 or else Legal_Field_ID_Barrier < ID then
        //                     raise Errors.Skill_Error
        //                     with Input.Parse_Exception
        //                       (Block_Counter,
        //                        "Found an illegal field ID: " & Integer'Image (ID));
        //                  end if;
        //
        //                  if Id = Legal_Field_ID_Barrier then
        //                     Legal_Field_ID_Barrier := Legal_Field_ID_Barrier + 1;
        //                     -- new field
        //                     declare
        //                        Field_Name : Types.String_Access := Strings.Get
        //                          (Input.V64);
        //                        T          : Field_Types.Field_Type;
        //
        //                        procedure Field_Restriction is
        //                           Count : Types.v64 := Input.V64;
        //                           Id    : Types.V64;
        //                        begin
        //                           for I in 1 .. Count loop
        //                              Id := Input.V64;
        //                              case Id is
        //                                 when 0 =>
        //                                    -- nonnull
        //                                    null;
        //
        //                                 when 1 =>
        //                                    -- default
        //                                    null;
        //
        //                                 when 3 =>
        //                                    -- range
        //                                    ID := Input.V64;
        //                                    ID := Input.V64;
        //
        //                                 when 5 =>
        //                                    -- coding
        //                                    ID := Input.V64;
        //
        //                                 when 7 =>
        //                                    -- CLP
        //                                    null;
        //
        //                                 when 9 =>
        //                                    -- one of
        //                                    null;
        //
        //                                 when others =>
        //                                    if Id <= 9 or else 1 = (Id mod 2) then
        //                                       raise Skill.Errors.Skill_Error
        //                                       with Input.Parse_Exception
        //                                         (Block_Counter,
        //                                          "Found unknown field restriction " &
        //                                            Integer'Image (Integer (Id)) &
        //                                            ". Please regenerate your binding, if possible.");
        //                                    end if;
        //
        //                                    Ada.Text_IO.Put_Line
        //                                      ("Skiped unknown skippable field restriction." &
        //                                         " Please update the SKilL implementation.");
        //
        //                              end case;
        //                           end loop;
        //
        //                           -- TODO results!!
        //                        end Field_Restriction;
        //                     begin
        //                        if null = Field_Name then
        //                           raise Errors.Skill_Error
        //                           with Input.Parse_Exception
        //                             (Block_Counter,
        //                              "corrupted file: nullptr in fieldname");
        //                        end if;
        //
        //                        T := Parse_Field_Type;
        //                        Field_Restriction;
        //                        End_Offset := Input.V64;
        //
        //                        declare
        //                           -- TODO restrictions
        //                           F : Field_Declarations.Field_Declaration :=
        //                                 E.Pool.Add_Field (ID, T, Field_Name);
        //                        begin
        //                           F.Add_Chunk
        //                             (new Internal.Parts.Bulk_Chunk'
        //                                (Offset,
        //                                 End_Offset,
        //                                 Types.V64 (E.Pool.Size),
        //                                 E.Pool.Blocks.Length)
        //                             );
        //
        //                        exception
        //                           when E : Errors.Skill_Error =>
        //                              raise Errors.Skill_Error
        //                              with Input.Parse_Exception
        //                                (Block_Counter, E,
        //                                 "failed to add field");
        //                        end;
        //                     end;
        //                  else
        //                     -- field already seen
        //                     End_Offset := Input.V64;
        //                     E.Pool.Data_Fields.Element (ID).Add_Chunk
        //                       (new Internal.Parts.Simple_Chunk'
        //                          (Offset,
        //                           End_Offset,
        //                           Last_Block.Count,
        //                           Last_Block.Bpo));
        //                  end if;
        //                  Offset := End_Offset;
        //
        //                  Field_Data_Queue.Append (FD_Entry'(E.Pool, ID));
        //               end;
        //            end loop;
        //         end;

        // reset counters and queues
        seenTypes.clear
        val resizeQueue = new ArrayBuffer[StoragePool[_, _]](typeCount)

        // number of fields to expect for that type in this block
        final class LFEntry(pool : StoragePool[_, _], count : Int)
        val localFields = new ArrayBuffer[LFEntry](typeCount)

        for (i ← 0 until typeCount) {
          // type definition
          val name = String.get(in.v64.toInt)

          def typeRestriction = {
            ???
            //            procedure Type_Restriction is
            //               Count : Types.v64 := Input.V64;
            //               Id : Types.V64;
            //            begin
            //               for I in 1 .. Count loop
            //                  Id := Input.V64;
            //                  case Id is
            //                  when 0 =>
            //                     -- unique
            //                     null;
            //
            //                  when 1 =>
            //                     -- singleton
            //                     null;
            //
            //                  when 2 =>
            //                     -- monotone
            //                     null;
            //
            //                  when others =>
            //                     if Id <= 5 or else  1 = (id mod 2) then
            //                        raise Skill.Errors.Skill_Error
            //                        with Input.Parse_Exception
            //                          (Block_Counter,
            //                           "Found unknown type restriction " &
            //                             Integer'Image (Integer (Id)) &
            //                             ". Please regenerate your binding, if possible.");
            //                     end if;
            //
            //                     Ada.Text_IO.Put_Line
            //                       ("Skiped unknown skippable type restriction." &
            //                          " Please update the SKilL implementation.");
            //
            //                  end case;
            //               end loop;
            //
            //               -- TODO result creation!!
            //            end Type_Restriction;
          }

          // check null name
          if (null == name)
            throw ParseException(in, blockCounter, null, "corrupted file, nullptr in typename")

          // check duplicate types
          if (seenTypes.contains(name))
            throw ParseException(in, blockCounter, null, s"duplicate definition of type $name")

          seenTypes.add(name)

          val count = in.v64
          val definition = typesByName.get(name).getOrElse {
            // TODO use result
            typeRestriction

            // super
            val superID = in.v64.toInt
            val superPool = if (0 == superID)
              null
            else {
              if (superID > types.size)
                throw ParseException(in, blockCounter, null, s"""Type $name refers to an ill-formed super type.
  found: $superID
  current number of types: ${types.size}""")
              else {
                val r = types(superID - 1)
                assert(r != null)
                r
              }
            }

            // allocate pool
            // TODO add restrictions as parameter
            val r = newPool(types.size + 32, name, superPool)
            types.append(r)
            typesByName.put(name, r)
            r
          }
          
          //note to self: we still need a block

          //               -- bpo
          //               if 0 /= Block.Count and then null /= Definition.Super then
          //                  Block.Bpo := Definition.Base.Data'Length + Input.V64;
          //               else
          //                  Block.Bpo := Definition.Base.Data'Length;
          //               end if;
          //
          //               pragma Assert(Definition /= null);
          //
          //               -- store block info and prepare resize
          //               Definition.Blocks.Append (Block);
          //               Resize_Queue.Append (Definition); -- <-TODO hier nur base pools einfügen!
          //               Local_Fields.Append (LF_Entry'(Definition.Dynamic, Input.V64));
          //            end;
          //         exception
          //            when E : Constraint_Error =>
          //               Skill.Errors.Print_Stacktrace(E);
          //               raise Errors.Skill_Error
          //               with Input.Parse_Exception
          //                 (Block_Counter, E, "unexpected corruption of parse state");
          //            when E : Storage_Error =>
          //               raise Errors.Skill_Error
          //               with Input.Parse_Exception
          //                 (Block_Counter, E, "unexpected end of file");
          //         end Type_Definition;
          ???
        }

        //         -- resize pools
        //         declare
        //            Index : Natural := Natural'First;
        //            procedure Resize (E : Skill.Types.Pools.Pool) is
        //            begin
        //               E.Dynamic.Resize_Pool;
        //            end;
        //         begin
        //            Resize_Queue.Foreach (Resize'Access);
        //         end;
        //
        //         -- parse fields
        //         Local_Fields.Foreach(Parse_Fields'Access);
        //
        //
        //         -- update field data information, so that it can be read in parallel or
        //         -- even lazy
        //         -- Process Field Data
        //         declare
        //            -- We Have To Add The File Offset To all Begins and Ends We Encounter
        //            File_Offset : constant Types.V64 := Input.Position;
        //            Data_End    : Types.V64 := File_Offset;
        //         begin
        //
        //            -- process field data declarations in order of appearance and update
        //            -- offsets to absolute positions
        //            while not Field_Data_Queue.Is_Empty loop
        //               declare
        //                  use type Skill.Field_Declarations.Field_Declaration;
        //
        //                  E : FD_Entry := Field_Data_Queue.Pop;
        //                  F : Skill.Field_Declarations.Field_Declaration :=
        //                        E.Pool.Data_Fields.Element (E.ID);
        //                  pragma Assert (F /= null);
        //
        //                  -- make begin/end absolute
        //                  End_Offset : Types.V64 :=
        //                                 F.Add_Offset_To_Last_Chunk (Input, File_Offset);
        //               begin
        //                  if Data_End < End_Offset then
        //                     Data_End := End_Offset;
        //                  end if;
        //               end;
        //            end loop;
        //            Input.Jump (Data_End);
        //         end;
      } catch {
        case e : SkillException ⇒ throw e
        case e : Exception      ⇒ throw ParseException(in, blockCounter, e, "unexpected foreign exception")
      }

      blockCounter += 1
      seenTypes = HashSet()
    }

    makeState(in.path(), mode, String, Annotation, types, typesByName)
  }
}