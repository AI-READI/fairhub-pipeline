import pydicom


# (dataset, seg_dic, oct_dic, op_dic)
def shared_functional_group_sequence(dataset, x, y, z):
    """
    Create the shared functional group sequence in the dataset.

    This function constructs the shared functional group sequence for a DICOM dataset,
    populating it with anatomical, reference, orientation, and pixel measurement information.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the functional groups are added.
        x (list): List containing data for constructing the functional groups.
        y (list): List containing referenced SOP class and instance UIDs for the first set of images.
        z (list): List containing referenced SOP class and instance UIDs for the second set of images.
    """

    purpose_of_reference_seq1 = pydicom.Sequence()
    purpose_of_reference_item1 = pydicom.Dataset()
    purpose_of_reference_item1.CodeValue = "121338"
    purpose_of_reference_item1.CodingSchemeDesignator = "DCM"
    purpose_of_reference_item1.CodeMeaning = "Anatomic Image"
    purpose_of_reference_seq1.append(purpose_of_reference_item1)

    purpose_of_reference_seq2 = pydicom.Sequence()
    purpose_of_reference_item2 = pydicom.Dataset()
    purpose_of_reference_item2.CodeValue = "121311"
    purpose_of_reference_item2.CodingSchemeDesignator = "DCM"
    purpose_of_reference_item2.CodeMeaning = "Localizer"
    purpose_of_reference_seq2.append(purpose_of_reference_item2)

    referenced_image_seq = pydicom.Sequence()
    referenced_image_item = pydicom.Dataset()

    referenced_image_item.ReferencedSOPClassUID = y[0]["00080016"].value
    referenced_image_item.ReferencedSOPInstanceUID = y[0]["00080018"].value
    referenced_image_item.PurposeOfReferenceCodeSequence = purpose_of_reference_seq1

    referenced_image_item1 = pydicom.Dataset()
    referenced_image_item1 = pydicom.Dataset()
    referenced_image_item1.ReferencedSOPClassUID = z[0]["00080016"].value
    referenced_image_item1.ReferencedSOPInstanceUID = z[0]["00080018"].value
    referenced_image_item1.PurposeOfReferenceCodeSequence = purpose_of_reference_seq2
    referenced_image_seq.append(referenced_image_item)
    referenced_image_seq.append(referenced_image_item1)

    purpose_of_reference_seq3 = pydicom.Sequence()
    purpose_of_reference_item3 = pydicom.Dataset()
    purpose_of_reference_item3.CodeValue = "113076"
    purpose_of_reference_item3.CodingSchemeDesignator = "DCM"
    purpose_of_reference_item3.CodeMeaning = (
        "Source Image for Image Processing Operation"
    )
    purpose_of_reference_seq3.append(purpose_of_reference_item3)

    source_image_seq = pydicom.Sequence()
    source_image_item = pydicom.Dataset()
    source_image_item.ReferencedSOPClassUID = y[0]["00080016"].value
    source_image_item.ReferencedSOPInstanceUID = y[0]["00080018"].value
    source_image_item.PurposeOfReferenceCodeSequence = purpose_of_reference_seq3
    source_image_seq.append(source_image_item)

    derivation_code_seq = pydicom.Sequence()
    derivation_code_item = pydicom.Dataset()
    derivation_code_item.CodeValue = "113076"
    derivation_code_item.CodingSchemeDesignator = "DCM"
    derivation_code_item.CodeMeaning = "Segmentationn"
    derivation_code_seq.append(derivation_code_item)

    derivation_image_seq = pydicom.Sequence()
    derivation_image_item = pydicom.Dataset()
    derivation_image_item.SourceImageSequence = source_image_seq
    derivation_image_item.DerivationCodeSequence = derivation_code_seq

    derivation_image_seq.append(derivation_image_item)

    plane_orientation_seq = pydicom.Sequence()
    plane_orientation_item = pydicom.Dataset()
    plane_orientation_item.ImageOrientationPatient = [1.0, 0.0, 0.0, 0.0, 0.0, 1.0]
    plane_orientation_seq.append(plane_orientation_item)

    # plane_position_seq = pydicom.Sequence()
    # plane_position_item = pydicom.Dataset()
    # plane_position_item.ImagePositionPatient = (
    #     y[0]["52009230"].value[0]["00209113"].value[0]["00200032"].value
    # )
    # plane_position_seq.append(plane_position_item)

    pixel_measures_seq = pydicom.Sequence()
    pixel_measures_item = pydicom.Dataset()
    a = y[0]["52009229"].value[0]["00289110"].value[0]["00280030"].value[1]
    b = y[0]["52009229"].value[0]["00289110"].value[0]["00280030"].value[1]
    pixel_measures_item.PixelSpacing = [a, b]
    pixel_measures_item.SliceThickness = (
        y[0]["52009229"].value[0]["00289110"].value[0]["00280030"].value[0]
    )
    pixel_measures_seq.append(pixel_measures_item)

    measurement_units_code_seq = pydicom.Sequence()
    measurement_units_code_item = pydicom.Dataset()
    measurement_units_code_item.CodeValue = "mm"
    measurement_units_code_item.CodingSchemeDesignator = "UCUM"
    measurement_units_code_item.CodeMeaning = "millimeter"
    measurement_units_code_seq.append(measurement_units_code_item)

    real_world_value_mapping_seq = pydicom.Sequence()
    real_world_value_mapping_item = pydicom.Dataset()
    real_world_value_mapping_item.LUTExplanation = "pixel height in mm"
    real_world_value_mapping_item.LUTLabel = "pixel in mm"
    real_world_value_mapping_item.DoubleFloatRealWorldValueLastValueMapped = y[0][
        "00280011"
    ].value
    real_world_value_mapping_item.DoubleFloatRealWorldValueFirstValueMapped = 0
    real_world_value_mapping_item.RealWorldValueIntercept = 0.0

    real_world_value_mapping_item.RealWorldValueSlope = (
        y[0]["52009229"].value[0]["00289110"].value[0]["00280030"].value[1]
    )

    real_world_value_mapping_item.MeasurementUnitsCodeSequence = (
        measurement_units_code_seq
    )
    real_world_value_mapping_seq.append(real_world_value_mapping_item)

    shared_func_groups_seq = pydicom.Sequence()
    dataset.SharedFunctionalGroupsSequence = shared_func_groups_seq

    shared_func_item = pydicom.Dataset()

    shared_func_item.ReferencedImageSequence = referenced_image_seq
    shared_func_item.DerivationImageSequence = derivation_image_seq
    shared_func_item.PlaneOrientationSequence = plane_orientation_seq
    # shared_func_item.PlanePositionSequence = plane_position_seq
    shared_func_item.PixelMeasuresSequence = pixel_measures_seq
    shared_func_item.RealWorldValueMappingSequence = real_world_value_mapping_seq

    shared_func_groups_seq.append(shared_func_item)


def per_frame_functional_groups_sequence(dataset):
    """
    Create the per-frame functional groups sequence in the dataset.

    This function constructs the per-frame functional groups sequence for a DICOM dataset,
    populating it with per-frame content, reference, ophthalmic, and position information.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the functional groups are added.

    """

    per_frame_functional_groups_seq = pydicom.Sequence()

    for i in range(2):

        frame_content_seq = pydicom.Sequence()
        frame_content_item = pydicom.Dataset()

        frame_content_item.StackID = str(i + 1)
        frame_content_item.InStackPositionNumber = i + 1
        frame_content_item.DimensionIndexValues = [i + i, i + 1]

        frame_content_seq.append(frame_content_item)

        segment_identification_seq = pydicom.Sequence()
        segment_identification_item = pydicom.Dataset()
        segment_identification_item.ReferencedSegmentNumber = i + 1
        segment_identification_seq.append(segment_identification_item)

        per_frame_functional_groups_item = pydicom.Dataset()
        per_frame_functional_groups_item.SegmentIdentificationSequence = (
            segment_identification_seq
        )
        per_frame_functional_groups_item.FrameContentSequence = frame_content_seq
        per_frame_functional_groups_seq.append(per_frame_functional_groups_item)

    dataset.PerFrameFunctionalGroupsSequence = per_frame_functional_groups_seq


def segment_sequence(dataset, x, y):
    """
    Create the segment sequence in the dataset.

    This function constructs the segment sequence for a DICOM dataset,
    populating it with anatomical structure and segmentation algorithm information.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the segment sequence is added.
        x (list): List containing data for constructing the segment sequence.
        y (list): List containing referenced SOP class and instance UIDs for the first set of images.
    """

    segment_seq = pydicom.Sequence()
    repeat = 2

    segment_number = [1, 2]
    segment_label = ["ILM", "RPE"]
    segmented_property_type_code_sequence_code_value = ["280677004", "128298"]
    segmented_property_type_code_sequence_coding_scheme_designator = ["SCT", "DCM"]
    segmented_property_type_code_sequence_code_meaning = [
        "ILM - Internal limiting membrane",
        "Surface of the center of the RPE",
    ]

    for i in range(repeat):
        segmented_property_catrgory_code_seq = pydicom.Sequence()
        segmented_property_catrgory_code_item = pydicom.Dataset()
        segmented_property_catrgory_code_item.CodeValue = "91723000"
        segmented_property_catrgory_code_item.CodingSchemeDesignator = "SCT"
        segmented_property_catrgory_code_item.CodeMeaning = "Anatomical Structure"
        segmented_property_catrgory_code_seq.append(
            segmented_property_catrgory_code_item
        )

        anatomic_region_seq = pydicom.Sequence()
        anatomic_region_item = pydicom.Dataset()
        anatomic_region_item.CodeValue = "5665001"
        anatomic_region_item.CodingSchemeDesignator = "SCT"
        anatomic_region_item.CodeMeaning = "Retina"
        anatomic_region_seq.append(anatomic_region_item)

        segmented_property_type_code_seq = pydicom.Sequence()
        segmented_property_type_code_item = pydicom.Dataset()
        segmented_property_type_code_item.CodeValue = (
            segmented_property_type_code_sequence_code_value[i]
        )
        segmented_property_type_code_item.CodingSchemeDesignator = (
            segmented_property_type_code_sequence_coding_scheme_designator[i]
        )
        segmented_property_type_code_item.CodeMeaning = (
            segmented_property_type_code_sequence_code_meaning[i]
        )
        segmented_property_type_code_seq.append(segmented_property_type_code_item)

        segment_item = pydicom.Dataset()
        segment_item.SegmentedPropertyCategoryCodeSequence = (
            segmented_property_catrgory_code_seq
        )
        segment_item.SegmentedPropertyTypeCodeSequence = (
            segmented_property_type_code_seq
        )
        segment_item.AnatomicRegionSequence = anatomic_region_seq

        segment_item.SegmentNumber = segment_number[i]
        segment_item.SegmentLabel = segment_label[i]
        segment_item.SegmentDescription = segment_label[i]
        segment_item.SegmentAlgorithmType = "AUTOMATIC"
        segment_item.SegmentAlgorithmName = "Zeiss"

        segment_seq.append(segment_item)

    dataset.SegmentSequence = segment_seq


def dimension_index_sequence(dataset, x, y):
    """
    Creates and adds a DimensionIndexSequence to the provided dataset.

    Parameters:
    dataset (pydicom.Dataset): The target DICOM dataset to which the sequence will be added.
    x: Placeholder parameter, not used in this function.
    y (list): A list containing DICOM datasets, with the first element used to extract the DimensionOrganizationUID.

    This function creates a DimensionIndexSequence with two items, each containing
    a DimensionOrganizationUID, DimensionIndexPointer, and FunctionalGroupPointer, and appends it to the dataset.
    """

    dimension_index_seq = pydicom.Sequence()

    dimension_index_item = pydicom.Dataset()
    dimension_index_item.DimensionOrganizationUID = (
        y[0]["00209221"].value[0]["00209164"].value
    )
    dimension_index_item.DimensionIndexPointer = "00209056"
    dimension_index_item.FunctionalGroupPointer = "00209111"
    dimension_index_seq.append(dimension_index_item)

    dimension_index_item1 = pydicom.Dataset()
    dimension_index_item1.DimensionOrganizationUID = (
        y[0]["00209221"].value[0]["00209164"].value
    )
    dimension_index_item1.DimensionIndexPointer = "00209057"
    dimension_index_item1.FunctionalGroupPointer = "00209111"
    dimension_index_seq.append(dimension_index_item1)

    dataset.DimensionIndexSequence = dimension_index_seq


def dimension_organization_sequence(dataset, x, y):
    """
    Creates and adds a DimensionOrganizationSequence to the provided dataset.

    Parameters:
    dataset (pydicom.Dataset): The target DICOM dataset to which the sequence will be added.
    x: Placeholder parameter, not used in this function.
    y (list): A list containing DICOM datasets, with the first element used to extract the DimensionOrganizationUID.

    This function creates a DimensionOrganizationSequence with one item containing
    the DimensionOrganizationUID and appends it to the dataset.
    """

    dimension_organization_seq = pydicom.Sequence()
    dimension_organization_item = pydicom.Dataset()

    dimension_organization_item.DimensionOrganizationUID = (
        y[0]["00209221"].value[0]["00209164"].value
    )

    dimension_organization_seq.append(dimension_organization_item)

    dataset.DimensionOrganizationSequence = dimension_organization_seq


def referenced_series_sequence(dataset, x, y, z):
    """
    Creates and adds a ReferencedSeriesSequence to the provided dataset.

    Parameters:
    dataset (pydicom.Dataset): The target DICOM dataset to which the sequence will be added.
    x: Placeholder parameter, not used in this function.
    y (list): A list containing DICOM datasets, with the first element used to extract the first ReferencedInstance.
    z (list): A list containing DICOM datasets, with the first element used to extract the second ReferencedInstance.

    This function creates a ReferencedSeriesSequence with two items, each containing
    a ReferencedInstanceSequence and SeriesInstanceUID, and appends it to the dataset.
    """

    referenced_instance_seq = pydicom.Sequence()
    referenced_instance_item = pydicom.Dataset()

    referenced_instance_item.ReferencedSOPClassUID = z[0]["00080016"].value
    referenced_instance_item.ReferencedSOPInstanceUID = z[0]["00080018"].value

    referenced_instance_seq.append(referenced_instance_item)

    referenced_instance_seq1 = pydicom.Sequence()
    referenced_instance_item1 = pydicom.Dataset()

    referenced_instance_item1.ReferencedSOPClassUID = y[0]["00080016"].value
    referenced_instance_item1.ReferencedSOPInstanceUID = y[0]["00080018"].value

    referenced_instance_seq1.append(referenced_instance_item1)

    referenced_series_seq = pydicom.Sequence()
    referenced_series_item = pydicom.Dataset()
    referenced_series_item.ReferencedInstanceSequence = referenced_instance_seq
    referenced_series_item.SeriesInstanceUID = z[0]["0020000E"].value
    referenced_series_seq.append(referenced_series_item)

    referenced_series_item1 = pydicom.Dataset()
    referenced_series_item1.ReferencedInstanceSequence = referenced_instance_seq1
    referenced_series_item1.SeriesInstanceUID = y[0]["0020000E"].value
    referenced_series_seq.append(referenced_series_item1)

    dataset.ReferencedSeriesSequence = referenced_series_seq
